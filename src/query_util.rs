use super::timestamp_util::*;
use lru::LruCache;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};

use chrono::{Utc,prelude::*};

pub enum QueryRequestType {
    Popular {
        lbound: i64,
        ubound: i64,
        length: i32,
    },
    Count {
        lbound: i64,
        ubound: i64,
    },
}

impl QueryRequestType {
    ///parses request from from http path 
    pub fn from_path(path: &String) -> Option<Self> {
        let ps = path.as_str();
        let tokens: Vec<_> = ps.split('/').rev().take(2).collect();
        let query = tokens[0];
        let query_type = tokens[1];
        
        Self::parse_query(query)
            .map(|(lbound,ubound,length)| match query_type {
                "count" => Some(Self::Count{lbound,ubound}),
                "popular" =>Some(Self::Popular{lbound,ubound,length:length as i32}),
                _ => None,
            })
            .flatten()
    }
    #[allow(unused_assignments)]
    fn parse_query(query: &str) -> Option<(i64, i64, i64)> {
        let mut lbound = 0;
        let mut ubound = 0;
        let mut num_results = 0i64;

        let tokens: Vec<_> = query.split('?').take(2).collect();
        println!("tokens = {:?}", tokens);

        if tokens.is_empty() || tokens.len() > 2 {
            return None;
        }
        if tokens.len() == 1 && tokens[0].len() == 0 {
            return None;
        }

        if tokens.len() == 2 {
            //parse size here
            num_results = tokens[1]
                .split('=')
                .rev()
                .next()
                .unwrap_or("1")
                .parse()
                .unwrap_or(1);
        }

        let time_tokens: Vec<_> = tokens[0].split(' ').collect();

        let mut hms: Vec<i64> = Vec::new();
        let ymd: Vec<i64> = time_tokens[0]
            .split('-')
            .map(|time| time.parse().unwrap())
            .collect();

        if time_tokens.len() == 2 {
            hms = time_tokens[1]
                .split(':')
                .map(|num| num.parse().unwrap())
                .collect();
        }
        let mut offset_table = 0;
        let mut date_lower = [[0u32; 3], [0; 3]];

        //ymd lower loop
        {
            let mut ymd_iter = ymd.iter();
            for e in date_lower[0].iter_mut() {
                *e = ymd_iter
                    .next()
                    .map(|&a| {
                        offset_table += 1;
                        a as u32
                    })
                    .unwrap_or(1);
            }
            let mut hms_iter = hms.iter();
            for e in date_lower[1].iter_mut() {
                *e = hms_iter
                    .next()
                    .map(|&a| {
                        offset_table += 1;
                        a as u32
                    })
                    .unwrap_or(0);
            }
        }

        let lower_date_time = Utc
            .ymd(date_lower[0][0] as i32, date_lower[0][1], date_lower[0][2])
            .and_hms(date_lower[1][0], date_lower[1][1], date_lower[1][2]);

        let offset_in_seconds = match offset_table {
            1 => 365 * 24 * 3600, //y
            2 => {//m
                let y = date_lower[0][0] as i32;
                let m = date_lower[0][1];
                let upper = if m == 12 {
                    Utc.ymd(y + 1, 1, 1)
                } else {
                    Utc.ymd(y, m + 1, 1)
                };
                let lower = Utc.ymd(y, m, 1);
                upper.signed_duration_since(lower).num_seconds()
            }
            3 => 24 * 3600,//d
            4 => 3600,//h
            5 => 60,//m
            _ => 1, //s

        };
        let upper_date_time: DateTime<Utc> =
            lower_date_time + chrono::Duration::seconds(offset_in_seconds);

        lbound = lower_date_time.timestamp();
        ubound = upper_date_time.timestamp();

        // println!("ymd_hms_lower = {:?}", date_lower);
        // println!("ymd = {:?}", ymd);
        // println!("hms = {:?}", hms);
        // println!("results = {}", num_results);

        Some((lbound, ubound, num_results))
    }
}




pub struct QueryResults {
    url_table: HashMap<String, u64>,
    top_results: Vec<(String, u64)>,
    pub max_results: u32,
}

impl QueryResults {
    pub fn new() -> Self {
        Self {
            url_table: HashMap::new(),
            top_results: Vec::new(),
            max_results: 1,
        }
    }

    pub fn clear(&mut self) {
        self.url_table.clear();
        self.top_results.clear();
    }

    pub fn get_distinct(&self)->u64{
        self.url_table.len() as u64
    }

    /// should be called for each chunk read
    pub fn add_url_to_stats(&mut self, url: String) {
        //returns updated hits
        if let Some(hits) = self.url_table.get_mut(&url) {
            *hits += 1;
        } else {
            self.url_table.insert(url.clone(), 1);
        }
    }

    pub fn as_list(&mut self) -> &[(String, u64)] {
        
        for (url, hits) in self.url_table.iter() {
            self.top_results.push((url.clone(), *hits));
            self.top_results.sort_by(|(_, a), (_, b)| b.cmp(a));
            if self.top_results.len() >= (self.max_results as usize + 1) {
                self.top_results.pop();
            }
        }
        let max_results  = (self.max_results as usize).min(self.top_results.len());
        &self.top_results[0..max_results]
    }
}

pub struct QueryExecutor {
    num_chunks: u64,
    chunk_ranges: Vec<(String, [i64; 2])>,
    cached_chunks: LruCache<String, TimestampChunk>,
}

impl QueryExecutor {
    pub fn new(num_chunks: u64) -> Self {
        //read and chache headers in advance to avoid touching disk and slowing down queries
        let chunk_ranges: Vec<(_, _)> = (0..num_chunks)
            .map(|k| format!("./transformed_database/globally_sorted_chunk_{}.dat", k))
            .map(|path| (path.clone(), read_chunk_header(path.as_str())))
            .collect();

        Self {
            num_chunks,
            chunk_ranges,
            //attempt to cache 60 percent of chunks at any given time
            cached_chunks: LruCache::new((num_chunks as usize * 6) / 10),
        }
    }
    
    /// This function collects data for a both queires at once
    ///`lbound` is inclusive time in seconds
    ///`ubound` is exclusive time in seconds
    pub fn scan_database(
        &mut self,
        lbound: i64,
        ubound: i64,
        results: &mut QueryResults,
        id_to_url_table: &HashMap<u64, String>,
    ) {
        self.iterate_over_chunks(lbound, ubound, |ts|{
            //url_id is the unique key needed to lookup the url string
            let url_id = ts.url_id;
            let url = id_to_url_table.get(&url_id).unwrap();
            results.add_url_to_stats(url.clone());
        });
    }



    /// A helper function that attempts to efficiently iterate over appropriate chunks given a range.\
    /// This function uses an LRU cache to keep as many chunks in main memory as possible to avoid\
    /// pulling in chunks from disk/ssd.
    fn iterate_over_chunks<Callback>(&mut self, lbound: i64, ubound: i64, mut cb: Callback)
    where
        Callback: FnMut(&TimeStampInfo),
    {
        let mut chunks_hit = 0;
        let mut total_chunks = 0;
        //split borrow struct here here
        println!("start query");
        let chunk_ranges = &mut self.chunk_ranges;
        let cached_chunks = &mut self.cached_chunks;
        chunk_ranges
            .iter()
            .filter(|(_, [chunk_lbound, chunk_ubound])| {
                is_overlapping(lbound, ubound, *chunk_lbound, *chunk_ubound)
            })
            .for_each(|(path, _)| {
                total_chunks += 1;
                let chunk = match cached_chunks.get(path) {
                    None => {
                        //cache miss so we pull the chunk straight into main memory from disk(slow)
                        let file = std::fs::read(path).expect("chunk not found");
                        let chunk: TimestampChunk =
                            bincode::deserialize(&file[..]).expect("deserialize failed");
                        //because chunk isnt in cache it is inserted 
                        cached_chunks.put(path.clone(), chunk);
                        //return a reference to chunk owned by LRU
                        cached_chunks.get(path).unwrap()
                    }
                    Some(chunk) => {
                        chunks_hit += 1;
                        chunk
                    }
                };

                let search_result = chunk
                    .timestamp_list
                    .binary_search_by_key(&lbound, |tsi| tsi.timestamp);

                let start_index = match search_result {
                    Ok(index) => index,
                    Err(index) => index.min(chunk.timestamp_list.len() - 1),
                };
                //walk forward until out of range
                for index in start_index..chunk.timestamp_list.len() {
                    let ts = &chunk.timestamp_list[index];
                    if  ts.timestamp < lbound || ts.timestamp >= ubound {
                        break;
                    }
                    cb(ts);
                }
                //walk backward until out of range
                for index in (0..start_index).rev() {
                    let ts = &chunk.timestamp_list[index];
                    if  ts.timestamp < lbound || ts.timestamp >= ubound {
                        break;
                    }
                    cb(ts);
                }
            });
        println!(
            "finished query [({}/{}) chunks were hit]",
            chunks_hit, total_chunks
        );
    }
}


//upper boundss are exclusive
//lower bounds are inclusive
fn is_overlapping(l0: i64, u0: i64, l1: i64, u1: i64) -> bool {
    let is_sperating = u1 <= l0 || u0 <= l1;
    is_sperating == false
}

//simply just reads header of a chunk and not the entire file which can be around 10-20 MBs
fn read_chunk_header(path: &str) -> [i64; 2] {
    let f = File::open(path).unwrap();
    let mut reader = BufReader::new(f);
    let mut header = [0i64, 2];
    reader
        .read(unsafe { std::slice::from_raw_parts_mut(header.as_mut_ptr() as *mut u8, 16) })
        .unwrap();
    header
}
