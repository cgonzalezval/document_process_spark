# Document classifier
This project is a PoC of a patent classifier using xml as raw data.


## Steps
0. Read data stored in azure blobs.
1. Process all the patent XMLs files and make sure all documents have at least a title and abstract written in English
2. Compute the 1000 most frequent words over the full text of all patents. English Stop Words have to be excluded.
3. Identify all patents that discuss energy consumption using using a binary document classifier. 
4. Analyze the energy consumption patents to figure out to which areas of topics (chemistry, biology, electrical 
   engineering, etc.) the patents might belong, by creating an unsupervised topic model (e.g. LDA) for the patents.
5. Persist the energy consumption patents in a new ElasticSearch index. The index has to contain the title, applicant(s), publication year, abstract, full text, and the topic words from the step before.