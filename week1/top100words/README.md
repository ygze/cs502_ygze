Top 100 words 

Usage: 
hadoop jar target/top100words-0.0.1-SNAPSHOT.jar com.example.Top100Words output_from_wordcout output_dir

Prerequirement:
The project count on the result of  wordcount, the output of which is the input for the project.

Algorithm:
Each mapper keeps a TreeMap to recorde the top 100 frequent words in local, and then send the local top 100 frequent words to reducers.
