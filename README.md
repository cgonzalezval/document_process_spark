# Text document classifier
This is a PoC for a patent classifier using xml as raw data.
The process will run once per week, with 110 million XML files.

## Requirements
0. Read data stored in azure blobs.
1. Process all the patent XMLs files and make sure all documents have at least a title and abstract written in English
2. Compute the 1000 most frequent words over the full text of all patents. English Stop Words have to be excluded.
3. Identify all patents that discuss energy consumption using a binary document classifier. 
4. Analyse the energy consumption patents to figure out to which areas of topics (chemistry, biology, electrical 
   engineering, etc.) the patents might belong, by creating an unsupervised topic model (e.g. LDA) for the patents.

## Design approach
### Language
I am going to use spark as it fits all the requirements for this challenge:
* Batch processing.  
* High volume of data.  
* Machine learning capabilities.  
* Production ready.  

I am going to choose to develop the process in python because of:
* its easy reading.  
* good developing speed.  
* availability packages to connect with all the systems and make scripting to automate tasks.  

As drawback:
* I’ll have to handle specifics data transformations with udfs which will impact in performance as long as they are 
  not pandas vectorized udfs. In the long term these udfs could be coded in scala and used in spark to solve this issue.
* I’ll have less capabilities to modify or extend spark codebase

### System
The process has to be developed in Azure so I’ll use Databricks and Azure Blob Storage.
This is a convenience solution but for a production environment HDInsights and Azure Data Lake Storage
should also be analysed as alternatives.  
In Databricks I am going to use the 6.4 version which uses Spark 2.4.5.
I am not choosing a newer version because spark-nlp is not spark 3.x.  

# Process description
The process has the following steps:  
1. Pre-process xml data.  
2. Parquetize data.  
3. Common text processing.  
4.  Filter English documents.  
5. Compute frequent words.  
6. Energy patents classifier.  
    - Feature engineering.  
    - Classifier training – analytical task.   
    - Classify patents.  
7. Clustering
   - Clustering training – analytical task

Each step will read data computed from previous steps and will store its output in a blob storage. 
In a production environment the I/O should be redirected to a local cluster storage and export only the desired data.  

#### Pre-process xml data
Input data is stored as tgz with thousands of xml files. This storage option is not adequate for our need because:
1.  Spark doesn’t support tar compression natively. It is possible to read the data as binary and process 
    it to decompressed but it is not a clean.  
2. Each file contains thousands of small xmls (1 register per file). This is not appropriate for 
   spark that perform at it’s best with a small number of splitable compressed big files.  

To solve these issues a python script has been develop. This process:  
1. Connects to azure blob and for each file:   
2. Download the file to local storage  
3. Uncompress the data  
4. Sanitizes text in text fields inside the xml  
5. Concatenates all xmls in one big xml  
6. Upload the data unzipped.  

Things to take into account:  

* This process doesn’t use spark so could be deployed in virtual machines. 
  It doesn’t need a Databrick cluster. In this implementation it is launched in a local cluster only 
  because convenience and time constraints. 
* This process doesn’t scale. This is because a time constraint for this task but it could be adjusted to
  process data in parallel using multiple threads per machine and multiple machines easily.
* The sanitize data step is very important because the data in the xmls is dirty and it will cause problems 
  in the following steps.  
    - The text fields contain what it looks like hml downloaded code so it has html tags as < /br>.
      These tags cause problems parsing the xmls so it’s mandatory to clean them.
    - The text fields contain invalid references  

 #### Parquetize data
The xml format can be good for data exchange but it is not appropriate for the analytical data processing of this task. 
This tasks requires a columnar storage with schema information to optimize I/O and cpu.  
Parquet storage has been chosen because it is the community default storage so it is compatible with practically 
all big data systems in case it is needed in the future.  

#### Common text processing
Text processing is cpu expensive so this step make some transformations that will be re-used in the next steps.
This is a storage/IO vs cpu compromise.   
Transformations:  
* Lowercase text  
* Remove English Stop Words  
* Lemmatize text  

#### Filter English documents
This step filters all data without an English abstract or title. 
The documents can have multiple titles in different languages so in the first place the process identify 
if one of this fields is in English and in the second place filters the data.  
The process also provides the distribution of languages per item so a monitorization t
ool could check for anomalies in production.  
It has been detected that there are titles without the _lang field so a language detector could be used to process 
these cases. This is part has not been implemented.  

#### Compute frequent words
For this task the process uses the text processed in the previous steps and computes the most frequent words. 
The result is stored as a csv in blob storage  

#### Energy patents classifier
To train the binary classifier 59 positive registers are provided and no negative data is provided.  
Negative data: to choose negative data I have used a LDA model with 50 topics over the initial data. 
The positives data scatters over 8 clusters so I have used clusters with no positive data to get a sample and 
use it as negative registers.  
* For this task there are better alternatives (hierarchical clustering) but it is not available in spark. 
  Sampling and a local pandas processing could be a good alternative.
* I have choosen a sample of 600 registers.  

To improve the efficiency of the classifier feature engineering is required.   
* Non-text fields: classification-ipcr 
  Data about classification-ipc could be useful for the classifier. 
  One patent can have multiple classifications, so I created a specific function to emulate the result of a 
  OneHotEncoderover the set of sections and sections and class. 
  Other subcategories are not included to avoid increase the dimensionality in huge numbers. 
*  Text fields: energy in text fields 
   I have created some flag variables to indicate if the word energy is present in the title, abstract or claims 
   In this point there are lots of options to improve analysing word distributions, including n-grams or bert encodings 
   The classifier is trained in a notebook as I think this task would be done by an analyst and not be run each week. 
   The trained classifier is stored in azure with a version and it is used by a script to classify all the registers.  

For the classifier I have used a simple approach: TF-IDF + LogisticRegression. Other options as 
Naïve Bayes or Random Forest should be tested and the hyperparameters should be tuned. 
It should be interesting to try semi-supervised models :  
  - https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2248178/  
  - One-class SVMs for Document Classification: https://www.jmlr.org/papers/volume2/manevitz01a/manevitz01a.pdf   

####- Clustering 
