#  **Server-Log-Data-Extraction-and-User-History-Database-Update**

### *Problem Statement*
##  <ins>*Task involves:</ins>*

  * Extracting email addresses and their corresponding dates from a server log file (mbox.txt).
* Transforming and cleaning the extracted data for consistency.
* Storing the processed data in MongoDB.
* Transferring the data from MongoDB to a relational database (SQLite).
* Running SQL queries to analyze and extract insights from the data.

## *Tasks Breakdown*
###  <ins>*Task 1: Extract Email Addresses and Dates*</ins>
* Read the log file (mbox.txt).
* Use Regex to extract email addresses.
* Extract timestamps associated with each email.
#### Example Regex Patterns:
        
        * Email extraction: r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        * Date extraction (assuming a common format in logs): r'\d{2} \w{3} \d{4} \d{2}:\d{2}:\d{2}'


###  <ins>*Task 2: Data Transformation*</ins>
 * Convert the extracted date into a standard format (e.g., YYYY-MM-DD HH:MM:SS).
* Structure the extracted data into a dictionary or DataFrame for easy storage.


###  <ins>*Task 3: Save Data to MongoDB*</ins>
* Connect to a MongoDB database using Python’s pymongo library.
* Create a collection named user_history.
* Insert extracted data in JSON format.
#### Example JSON structure for MongoDB:
          
          json
         
          {
              "email": "example@gmail.com",
              "date": "2024-02-24 15:30:45"
          }
### <ins>*Task 4: Database Connection and Data Upload*</ins>
* Fetch data from MongoDB.
* Connect to SQLite (or any SQL database) using SQL
* Create a user_history table with appropriate constraints.
* Insert the cleaned data into SQL

