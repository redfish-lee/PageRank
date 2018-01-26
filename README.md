## Page Rank
### What is page rank?
PageRank (PR) is an algorithm used by Google Search to rank websites in their search engine results.
- It works by counting the number and quality of links to a page to determine a rough estimate of how important the website is.
- The assumption is that more important websites are likely to receive more links from other websites.

### What is Hadoop?
[Apache Hadoop](https://hadoop.apache.org/) is an open-source software framework used for **distributed storage** and processing of dataset of **big data** using the **MapReduce** programming model. The project includes these modules:
- Hadoop Distributed File System (HDFS): A distributed file system to access application data.
- Hadoop YARN: A framework for job scheduling and cluster resource management.
- Hadoop MapReduce: A YARN-based system for parallel processing of large data sets.

<center>
    <img src="http://social.technet.microsoft.com/wiki/cfs-filesystemfile.ashx/__key/communityserver-components-imagefileviewer/communityserver-wikis-components-files-00-00-00-00-05/7848.TheHadoopEcosystem.png_2D00_550x0.png">
</center>

#### MapReduce
A MapReduce program is composed of a `Map()` method that performs filtering and sorting and a `Reduce()` method that performs a summary operation. Each mapper and reducer may execute on different nodes, so what each mapper and reducer would get should be implemented by programmer.
example:
![source](https://sundar5.files.wordpress.com/2010/03/mapreduce-e1269190940722.png)
- `mapper` write data in `<key, value>` format to context.
- `partitioner` decide how to partition different keys to which reducers.
- `reducer` get multiple key and their grouped values in iterable type.

#### HDFS
HDFS (Hadoop Distributed File System) stores large files across multiple machines and can be access by a single directory system.
```bash
### List contents on pageRank directory
$ hdfs dfs -ls -R pageRank
pageRank
|-- _SUCCESS
|-- part-00000
|-- part-00001
`-- part-00002

### merge pageRank directory content to a single file
$ hdfs dfs -getmerge pageRank pagerank.out
```

### Database
#### Wiki text format
[Wikitext](https://en.wikipedia.org/wiki/Help:Wikitext) is our input format
The input file on HDFS is a normal text file in wikitext format with multiple lines. Each line contains one page which is enclosed in `<page>` and `</page>`. There are only two attributes we need to consider: **page title** and **page links**

#### Page title
- Placed between `<title>` and `</title>`. 
- The first character of a title is always in upper case.
- Since title is part of an XML text, the real title text need to be un-escaped as follows:

|input string in title text|un-escaped character|
|--|--|
|&lt;| < |
|&gt;| > |
|&amp; | & |
|&quot; | " |
|&apos; | ' | 
- For example, the title string **`Ulmus &apos;Nire-keyaki&apos;`** need to be converted to **`Ulmus 'Nire-keyaki'`**.

#### link
- Placed between `[[` and `]]`, which defines what page it points to and the shown text of the link.
- To simplify the processing and be more specific, we define a link to another page as follows:
    - `[[` means what follows is a target page title.
    - The page title is case-sensitive except the first character.
    - Only capitalize the first character if it’s from `a - z`
    - The first `]]`, vertical bar `|` or sharp sign `#`,  it meets afterwards means the end of the target page title.
    - Note that links which points to a nonexistent page is not considered a valid link.

- [Wiki Download](https://en.wikipedia.org/wiki/Wikipedia:Database_download)

## Implementation
### Structure
```bash
|-- Makefile
|-- README.md
|-- bin
|-- execute.sh
|-- logs
|   |-- input-100M.out
|   `-- input-1G.out
`-- src
    |-- PageRanking.java
    |-- calculate
    |   |-- CalcMapper.java
    |   `-- CalcReduce.java
    |-- parse
    |   |-- ParseMapper.java
    |   |-- ParsePartitioner.java
    |   `-- ParseReducer.java
    `-- result
        |-- SortMapper.java
        |-- SortPair.java
        |-- SortPartitioner.java
        `-- SortReducer.java

6 directories, 15 files
```
### Page Rank Equation
$$\LARGE PR(x) = (1 - \alpha)(\frac{1}{N} ) +\alpha \sum_{i=1}^{n}\frac{PR(t_i)}{C(t_i)} + \alpha \sum_{j=1}^{m} \frac{PR(d_j)}{N}$$

- α: damping factor
- N: number of total pages
- n: number of pages points to *`x`*
- C: number of outlinks of the page which points to *`x`*
- m: number of dangling pages (dangling page is a page without outlinks)


### Troubleshooting
- **What's the difference of `TextInputFormat` and  `KeyValueTextInputFormat`**?
    - The `TextInputFormat` class converts every row of the source file into key/value types where the BytesWritable key represents the offset of the record and the Text value represents the entire record itself.
    - The `KeyValueTextInputFormat` is useful to fetch every source record as Text/Text pair where the key/value were populated from the record by splitting the record with a fixed delimiter.
    - [Stack Overflow](https://stackoverflow.com/a/29915751/8426713)

Consider the Below file contents,
```
### record input files
AL#Alabama
AR#Arkansas
FL#Florida

### TextInputFormat (key, value)
0    AL#Alabama
14   AR#Arkansas
23   FL#Florida

### KeyvalueTextInputFormat (key, value)
### ("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "#")
AL    Alabama
AR    Arkansas
FL    Florida
```

#### Reference Links
- [Google MapReduce OSDI Paper](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf)
- [Wiki Page Rank with Hadoop](http://blog.xebia.com/wiki-pagerank-with-hadoop/)
