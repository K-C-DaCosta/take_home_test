use std::cell::RefCell;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use take_home_test::{data_transform_util::*, query_util::*};

/*
At first glance the task of the assignment is simple, however, if one takes into consideration
ram and disk constraints things get more complicated.

My approach was to parse the textfile and convert timstamps and urls into a compact binary format.
Each distinct URL is assigned an ID, reducing redundancy in the dataset.
The text dataset is broken up into K chunks and are sorted by timestamp.
These sorted chunks are then merged in ascending order using a K-Way merge (no min-heap is used for that though).

Finally, when scanning the sorted chunks for queries a LRU cache is used which allows me to:
    -avoid fetches from disk (speeding up query)
    -avoid hogging Ram

*/

fn main() {
    let mut transformer = DataBaseTransformer::new();
    let chunk_count = transformer.transform_database(250000, 250000);

    let id_to_url_table = transformer.build_id_table();
    let executor = QueryExecutor::new(chunk_count);
    let results = QueryResults::new();
    let query_pointer = Arc::new(Mutex::new(RefCell::new((
        id_to_url_table,
        executor,
        results,
    ))));

    let pointer_clone = query_pointer.clone();
    start_primitive_web_server(move |path, message| {
        let query_pointer = (&pointer_clone).clone();
        let path = path.replace("%20", " ");

        match QueryRequestType::from_path(&path) {
            Some(QueryRequestType::Count { lbound, ubound }) => {
                let lock = query_pointer.lock().unwrap();
                let (_, executor, results) = &mut *lock.borrow_mut();
                results.clear();
                results.max_results = 1;
                let t0 = Instant::now();
                executor.scan_database(lbound, ubound, results);
                let distinct = results.get_distinct();
                let dt = t0.elapsed().as_millis();

                message.push_str(format!("{{ count: {} }}\n", distinct).as_str());

                println!("{}", format!("query took {} ms...\n", dt).as_str());
            }
            Some(QueryRequestType::Popular {
                lbound,
                ubound,
                length,
            }) => {
                let lock = query_pointer.lock().unwrap();
                let (id_table, executor, results) = &mut *lock.borrow_mut();
                results.clear();
                results.max_results = length as u32;
                let t0 = Instant::now();
                executor.scan_database(lbound, ubound, results);
                message.push_str("{\n\t \"queries\": [\n");
                results.as_list_cb(id_table, |k, len, url, hits| {
                    if k != len - 1 {
                        message.push_str(
                            format!("\t   {{ \"query\": \"{}\",\"count\":\"{}\"}},\n", url, hits)
                                .as_str(),
                        );
                    } else {
                        message.push_str(
                            format!("\t   {{ \"query\": \"{}\",\"count\":\"{}\"}}\n", url, hits)
                                .as_str(),
                        );
                    }
                });
                message.push_str("\t]\n}\n");
                let dt = t0.elapsed().as_millis();

                println!("{}", format!("query took {} ms...\n", dt).as_str());
            }
            _ => (),
        }
    });
}

// Spins up a extremely primitive webserver
fn start_primitive_web_server<Callback>(cb: Callback)
where
    Callback: FnMut(String, &mut String) + Send + Clone + 'static,
{
    //spin up an extremely primitive http server for sending back queries
    let listener = TcpListener::bind("localhost:8080").expect("bind failed");
    for client_res in listener.incoming() {
        match client_res {
            Ok(mut client) => {
                let mut cb_clone = cb.clone();
                std::thread::spawn(move || {
                    //read http request(I just assume its a request)
                    let mut buffer = [0u8; 1024];
                    let mut http_request_bytes = Vec::<u8>::new();
                    if let Ok(size) = client.read(&mut buffer[..]) {
                        if size > 0 {
                            buffer[0..size].iter().for_each(|&byte| {
                                http_request_bytes.push(byte);
                            });
                        }
                    }

                    //parse request
                    let mut headers = [httparse::EMPTY_HEADER; 16];
                    let mut request = httparse::Request::new(&mut headers[..]);
                    let mut path = String::new();
                    if let Ok(_) = request.parse(&http_request_bytes[..]) {
                        let rpath = request.path.unwrap_or("");
                        path = rpath.to_string();
                    };

                    //gather response
                    let mut message = String::new();
                    cb_clone(path, &mut message);

                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
                        message.len(),
                        message
                    );

                    // write response back out to client
                    client.write(&response.as_bytes()[..]).unwrap();
                });
            }
            Err(err) => {
                println!("{}", err);
            }
        }
    }
}
