use chrono::prelude::*;
use chrono::Utc;
use std::collections::{HashMap};
use std::fs::File;
use std::io::{prelude::*, BufReader, BufWriter, Bytes};
use std::mem;



use super::timestamp_util::*;


//keeps a global url->id hash table maintained for entire
//database
pub struct DataBaseTransformer {
    url_table: HashMap<String, u64>,
    distinct_url_count: u64,
}

///Custom iterator that reads 16 bytes at a time into a timestamp
///and does a reinterpret_cast into a Timestamp
struct TimestampIterator<T> {
    byte_iterator: Bytes<T>,
}
impl<T> TimestampIterator<T> {
    pub fn new(iter: Bytes<T>) -> Self {
        Self {
            byte_iterator: iter,
        }
    }
}
impl<T: Read> Iterator for TimestampIterator<T> {
    type Item = TimeStampInfo;
    fn next(&mut self) -> Option<Self::Item> {
        let mut timestamp_bytes = [0u8; 16];
        for dst_byte in timestamp_bytes.iter_mut() {
            match self.byte_iterator.next() {
                Some(Ok(src_byte)) => {
                    *dst_byte = src_byte;
                }
                _ => return None,
            }
        }
        let timestamp = unsafe { *(timestamp_bytes.as_ptr() as *const TimeStampInfo) };
        Some(timestamp)
    }
}

impl DataBaseTransformer {
    pub fn new() -> Self {
        Self {
            url_table: HashMap::new(),
        
            distinct_url_count: 0,
        }
    }
    pub fn build_id_table(&mut self)->HashMap<u64,String>{
        let mut id_to_url_table = HashMap::new();
        for (string, id) in self.url_table.iter(){
            id_to_url_table.insert(*id, string.clone());
        }
        self.url_table.clear();
        id_to_url_table
    }

    ///parses text file into more compact binary format and preforms and external sort on the transformed
    ///database. This function will return the number of globally sorted chunks.
    pub fn transform_database(
        &mut self,
        max_local_chunk_size: u64,
        max_global_chunk_size: u64,
    ) -> u64 {
        let mut globally_sorted_chunk_count = 0;
        let locally_sorted_chunk_count =
            self.compute_locally_sorted_compact_chunks(max_local_chunk_size);

        //The dataset is potentially massive so there is no way to sort the entire thing at once in RAM.
        //Because of the data size a k-way merge is needed to globally sort all of the data
        let mut buf = vec![0u8; 8];
        let mut local_chunks: Vec<_> = (0..locally_sorted_chunk_count)
            .map(|k| {
                //place file handle into a bufreader for quicker reads
                let path = format!("./transformed_database/locally_sorted_chunk_{}.dat", k);
                let mut freader = BufReader::new(File::open(path).unwrap());

                //seek past the header (we dont care about this for now)
                freader
                    .seek(std::io::SeekFrom::Current(
                        mem::size_of::<TimestampChunkHeader>() as i64,
                    ))
                    .unwrap();

                //reads size(total numer of timestamps in chunk)
                //all reads past this point is timestamp data
                freader.read_exact(&mut buf[..]).unwrap();

                //The tuple is of the format:
                //(file reader for chunk, number of timestamps in chunk, byte-buffer, number of timestamps read )
                freader
            })
            .collect();

        let mut globally_sorted_chunk = TimestampChunk::new();
        


        //collect peekable buffers
        let mut iter_list: Vec<_> = local_chunks
            .iter_mut()
            .map(|freader| TimestampIterator::new(freader.bytes()).peekable())
            .collect();

        loop {
            //if every iterator peek() evaluates to a None
            //it means all iterators are empty
            let all_none = iter_list
                .iter_mut()
                .map(|it| it.peek().is_none())
                .all(|is_none| is_none);

            //check that globally sorted chunk is complete and ready to be written to disk
            if all_none || globally_sorted_chunk.timestamp_list.len() > max_global_chunk_size as usize {
                Self::flush_global_chunk(&mut globally_sorted_chunk,&mut globally_sorted_chunk_count);
                if all_none{
                    break; 
                }
            }

            //compute the smallest timestamp that each buffer
            //currently points to
            let mut smallest_timestamp = i64::MAX;
            let mut smallest_iter_index = 0;
            for (k, it) in iter_list.iter_mut().enumerate() {
                let ts = it
                    .peek()
                    .map(|&ts_info| ts_info.timestamp)
                    .unwrap_or(i64::MAX);
                if ts < smallest_timestamp {
                    smallest_timestamp = ts;
                    smallest_iter_index = k;
                }
            }

            //push smallest timestamp into the list here
            let smallest_timestamp = iter_list[smallest_iter_index].next().unwrap();
            globally_sorted_chunk
                .timestamp_list
                .push(smallest_timestamp);
        }


        //remove local chunks from disk 
        for chunk in local_chunks{
            std::mem::drop(chunk);
        }
        for k in 0..locally_sorted_chunk_count{
            let path = format!("./transformed_database/locally_sorted_chunk_{}.dat", k);
            std::fs::remove_file(path).unwrap();
        }
        
        globally_sorted_chunk_count
    }

    fn flush_global_chunk(globally_sorted_chunk:&mut TimestampChunk,globally_sorted_chunk_count:&mut u64) {
        if globally_sorted_chunk.timestamp_list.len() == 0 {
            return;
        }

        //update header compute new min/max times
        globally_sorted_chunk.header.min_time = globally_sorted_chunk
            .timestamp_list
            .first()
            .unwrap()
            .timestamp;

        globally_sorted_chunk.header.max_time = globally_sorted_chunk
            .timestamp_list
            .last()
            .unwrap()
            .timestamp;

        //convert chunk to bytes
        let chunk_bytes = bincode::serialize(&globally_sorted_chunk).unwrap();

        let global_chunk_path = format!(
            "./transformed_database/globally_sorted_chunk_{}.dat",
            globally_sorted_chunk_count
        );

        let mut chunk_file_handler = File::create(global_chunk_path).unwrap();
        chunk_file_handler
            .write(&chunk_bytes[..])
            .expect("globally sorted chunk failed to write");

        //clear globally sorted chunk
        globally_sorted_chunk.timestamp_list.clear();

        //after writing a globally sorted chunk increment count
        *globally_sorted_chunk_count += 1;
    }

    ///Parses hn_logs.tsv into a 'chunk' which is in a more compact binary format.\
    ///Chunks simply contain a list of (timestamp,url) 2-tuples\
    ///the tuples are exactly 16 bytes
    fn compute_locally_sorted_compact_chunks(&mut self, max_chunk_size: u64) -> u64 {
        let file = File::open("./assignment/hn_logs.tsv")
            .expect("Error: file probably doesn't exist / wrong permissions or something..");
        let reader = BufReader::new(file);

        let mut lines_iter = reader
            .lines()
            .enumerate()
            .map(|(k, res)| (k, res.unwrap_or(String::new())))
            .peekable();

        let mut chunk_count = 0;
        while let Some((_line_num, _line)) = lines_iter.peek() {
            //I read the TSV file 250k lines at a time
            let mut timestamp_list = self.parse_raw_data_chunk(&mut lines_iter, max_chunk_size);

            //sort timestamps in acending order for fast range query(binary search)
            //this is a sort that is local to each chunk
            timestamp_list.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

            let chunk = TimestampChunk {
                header: TimestampChunkHeader {
                    min_time: timestamp_list
                        .first()
                        .map(|a| a.timestamp)
                        .unwrap_or(i64::MAX),
                    max_time: timestamp_list
                        .last()
                        .map(|a| a.timestamp)
                        .unwrap_or(i64::MIN),
                },
                timestamp_list,
            };

            let chunk_bytes = bincode::serialize(&chunk).expect("serialization error");

            let chunk_name = format!(
                "./transformed_database/locally_sorted_chunk_{}.dat",
                chunk_count
            );

            let chunk_file_handler =
                File::create(chunk_name).expect("create failed for some reason");
            let mut writer = BufWriter::new(chunk_file_handler);

            //the writing of the locally sorted chunk to disk happens here
            writer.write(&chunk_bytes[..]).unwrap();

            chunk_count += 1;
        }
        chunk_count
    }

    /// parses a chunk of original datafile into a more compact format in main memory
    /// which will then be written to disk for fast queries
    /// The function outputs a table that contains local-chunk info about every URL
    fn parse_raw_data_chunk(
        &mut self,
        lines_iter: &mut impl Iterator<Item = (usize, String)>,
        max_chunk_size: u64,
    ) -> Vec<TimeStampInfo> {
        let mut line_count = 0;

        //convert each line to: [timestamp](i64) [url_id](u64)
        //each entry is about 16 bytes now which is significantly smaller than
        //raw text
        let mut timestamp_list: Vec<TimeStampInfo> = Vec::new();

        //I parse the file line-by-line here
        for (_line_index, line) in lines_iter {
            let mut tokens = line.split('\t');

            let (date, time) = tokens
                .next()
                .map(|date_time| {
                    let mut tokens = date_time.split(' ');
                    let date = tokens.next().unwrap_or("0-0-0");
                    let time = tokens.next().unwrap_or("0:0:0");
                    (date, time)
                })
                .unwrap_or(("0-0-0", "0:0:0"));

            let url = tokens.next().unwrap().to_string();

            //parse date to integer 3-tuple
            let ymd: Vec<u32> = date
                .split('-')
                .map(|val| val.parse().ok().unwrap_or_default())
                .collect();

            //parse time to integer 3-tuple
            let hms: Vec<u32> = time
                .split(':')
                .map(|val| val.parse().ok().unwrap_or_default())
                .collect();

            //convert date to timestamp thanks to chrono a timezone package
            let timestamp = Utc
                .ymd(ymd[0] as i32, ymd[1], ymd[2])
                .and_hms(hms[0], hms[1], hms[2])
                .timestamp();

            //counts how many times each url occurs in text file and its timestamp
            match self.url_table.get_mut(&url) {
                //if im seeing url for first time just insert into table
                None => {
                    self.url_table.insert(url, self.distinct_url_count);

                    timestamp_list.push(TimeStampInfo {
                        timestamp,
                        url_id: self.distinct_url_count,
                    });

                    self.distinct_url_count += 1;
                }
                // if url has been scanned already just fetch the url_id and push a (timstamp,id) tuple
                Some(&mut url_id) => {
                    timestamp_list.push(TimeStampInfo { timestamp, url_id });
                }
            };

            line_count += 1;
            if line_count >= max_chunk_size {
                return timestamp_list;
            }
        }
        timestamp_list
    }
}
