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
just extract hn_logs.gz and make sure the extracted file is in the `./assignment` directory.
Then do a :
```
cargo run --release 
```
The server will bind to localhost:8080

# Conclusion:

I'm honestly not happy with my solution. I'm pretty sure that I would definitely get rejected. My solution runs really slow and I, like the anon who got rejected,  could not figure out a way to avoid the worst case scenario (scanning the entire database) for queries like:
``` 
.../popular/2015?size=3 
``` 