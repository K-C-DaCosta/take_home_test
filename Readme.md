# Technical Interview Question 
For more info on the Question decompress the `./assignment/subject.7z`. 

# What Anon was rejected for:
- He loaded the entire database into memory 
- not well commented
- import *  
- and some other shit that i cant remember

# What I did: 
- did a k-way merge on sorted chunks of the database in order to avoid loading the entire file into memory.
- I also use an LRU cache to avoid fetching chunks from disk
  
# How to run 
just extract `./assignment/hn_logs.tsv.gz` and make sure the extracted file is in the same directory as the compressed file.
Then do a :
```
cargo run --release 
```
The server will bind to localhost:8080

# Conclusion:
[EDIT] - So I figured out that string cloning was dramatically reducing the speed of my queries. The newest commit almost
completely eliminates cloning and now scanning the entire dataset of 1 mil + entries takes ~ 210 ms ( before it took 800 ms ). My implementation doesn't precompute queries, but if I did do that I would probably see even bigger speed gains. I'm much happier with the current solution now that I have a better Idea of where to look to speed things up.
--

I'm honestly not happy with my solution. I'm pretty sure that I would definitely get rejected. My solution runs really slow and I, like the anon who got rejected,  could not figure out a way to avoid the worst case scenario (scanning the entire database) for queries like:
``` 
.../popular/2015?size=3 
``` 

