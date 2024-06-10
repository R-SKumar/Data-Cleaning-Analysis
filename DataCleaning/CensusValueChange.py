
import pandas as pd
import numpy as np
import os
from docx import Document
from pymongo.mongo_client import MongoClient
import mysql.connector

fileData = None
def valueChange(sourcePath, outputPath, sheetName, columnInfo, methodRun, searchValue):
    try:
        # Check if the input file exists
        if not fileExists(sourcePath):
            return "The input file does not exist"
        
        # Check the output file directory
        if not directoryExists(outputPath):
            return "Failed to create output directory"
        
        if len(methodRun) == 0:
            return "Input run method not available"
        
        # Load the Excel file
        fileData = pd.read_excel(sourcePath, sheet_name=sheetName)
        
        # Get the list of columns
        availableColumns = list(fileData.columns)
        
        # Check any missing column
        if columnInfo != None:
            misColumn = getMissingColumn(availableColumns,columnInfo)
            if misColumn != 'Success':
                return misColumn
        
        if methodRun == 'Rename':
            # Rename the file header columns
            fileData.rename(columns=columnInfo, inplace=True)            
        elif methodRun == 'Capitalize':
            # Capitalize the first letter of each word in specified columns
            for column in columnInfo:
                fileData[column] = fileData[column].apply(lambda x: ' '.join(
                    word.capitalize() if word.lower() not in searchValue else word.lower() for word in x.split()
                ))
        elif methodRun == 'District':
            # Change the district name based upon input
            for item in searchValue:
                replaceValue , dictValue = next(iter(item.items()))
                stateValue, districtValue = next(iter(dictValue.items()))
                
                distValues = '|'.join(districtValue)
                
                # Rename State/UT for districts
                fileData.loc[
                    (fileData[columnInfo[0]].str.lower() == stateValue.lower()) & 
                    # (fileData[[columnInfo[1]].isin(districtValue)),
                    (fileData[columnInfo[1]].str.contains(distValues, case=False, na=False)),
                    columnInfo[0]
                ] = replaceValue
        elif methodRun == 'Data':
            initMisValue = fileData.isna().mean() * 100
            updatePopulationData(fileData)
            misColumnval = {('Population','Male','Female'),
                            ('Literate','Literate_Male','Literate_Female'),
                            ('SC','Male_SC','Female_SC'),
                            ('ST','Male_ST','Female_ST'),
                            ('Workers','Male_Workers','Female_Workers'),
                            ('Workers','Main_Workers','Marginal_Workers'),
                            ('Households','Households_Rural','Households_Urban'),
                            ('Total_Education','Literate_Education','Illiterate_Education')
                            }            
            updateMissingValues(fileData,misColumnval)
            multiMisColumn = [
                {'Workers': {'Population': ['Non_Workers','Cultivator_Workers','Agricultural_Workers','Household_Workers','Other_Workers']}},
                {'Religion': {'Population': ['Hindus','Muslims','Christians','Sikhs','Buddhists','Jains','Others_Religions','Religion_Not_Stated']}},
                {'Literate': {'Literate_Education': ['Below_Primary_Education','Primary_Education','Middle_Education','Secondary_Education','Higher_Education','Graduate_Education','Other_Education']}},
                {'AgeGroup': {'Population': ['Young_and_Adult','Middle_Aged','Senior_Citizen','Age_Not_Stated']}}
            ]
            fileData = updateMissingValuesMultiColumn(fileData,multiMisColumn)            
            endMisValue = fileData.isna().mean() * 100
            return savetoMongoDB(fileData)
        else:
            return "Method not matched to run"
        
        # Save the updated column name to a new Excel file
        fileData.to_excel(outputPath, index=False, sheet_name=sheetName)        
        return "Success"
    except Exception as e:
        return f"An error occurred: {e}"

def updatePopulationData(fileData):
    # mis = fileData.isna().mean() * 100
    # Calculate the sum of available worker columns for the alternative population calculation
    fileData['workers_Sum'] = fileData[['Non_Workers', 'Cultivator_Workers', 'Agricultural_Workers',
                                    'Household_Workers', 'Other_Workers']].sum(axis=1, skipna=True)    
    # Update NaN values in Population
    fileData['Population'] = np.where(
            pd.isna(fileData['Population']),
                np.where(
                pd.notna(fileData['Male']) & pd.notna(fileData['Female']),
                fileData['Male'] + fileData['Female'],
                fileData['workers_Sum']
            ), fileData['Population']
        )
    # Drop the temporary column
    fileData.drop(columns=['workers_Sum'], inplace=True)

def updateMissingValues(fileData, keyCollection):
    # if len(keyValue) == 0:
    #     return "Key value not available"
    for keyValue in keyCollection:    
        fileData[keyValue[0]] = np.where(
            pd.isna(fileData[keyValue[0]]),
            fileData[keyValue[1]] + fileData[keyValue[2]],
            fileData[keyValue[0]]
            )    
        fileData[keyValue[1]] = np.where(
            pd.isna(fileData[keyValue[1]]),
            fileData[keyValue[0]] - fileData[keyValue[2]],
            fileData[keyValue[1]]
            )
        fileData[keyValue[2]] = np.where(
            pd.isna(fileData[keyValue[2]]),
            fileData[keyValue[0]] - fileData[keyValue[1]],
            fileData[keyValue[2]]
            )
    return fileData 

def updateMissingValuesMultiColumn(fileData, columnInfo):
    for item in columnInfo:
        replaceValue, dictValue = next(iter(item.items()))
        stateValue, districtValue = next(iter(dictValue.items()))
        
        # Calculate the sum of the specified columns
        fileData['workers_Sum'] = fileData[districtValue].sum(axis=1)
        
        # Identify NaN values and update them
        for col in districtValue:
            if fileData[col].isna().any():
                fileData[col] = fileData[col].fillna(fileData[stateValue] - fileData['workers_Sum'])
        
        # Drop the temporary column
        fileData.drop(columns=['workers_Sum'], inplace=True)
    return fileData

def getMissingColumn(availableColumn, columnInfo):
    # Check the column type
    if isinstance(columnInfo, dict):
        colCheck = columnInfo.keys()
    elif isinstance(columnInfo, (set, list)):
        colCheck = columnInfo
    else:
        return "Invalid input type. Please provide a dictionary, set or list"
    
    missingColumns = [col for col in colCheck if col not in availableColumn]
    if missingColumns:
        return f"The columns were not found in the file: {', '.join(missingColumns)}"
    else:
        return "Success"

def getDocxData(sourcePath, keyValue, replaceValue):
    if not fileExists(sourcePath):
        print("The input file does not exist.")
        return None
        
    try:
        doc = Document(sourcePath)
        textValue = {replaceValue:{keyValue:[]}}
        for i, paragraph in enumerate(doc.paragraphs):                                    
            textValue[replaceValue][keyValue].append(paragraph.text)
        return textValue
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def fileExists(inputFile):
    # Check if a file exists at the given path
    if not os.path.exists(inputFile):
        return False
    else:
        return True

def directoryExists(inputFile):
    # The directory for the given file path exists, create it if it doesn't.
    try:
        directory = os.path.dirname(inputFile)
        if directory and not os.path.exists(directory):
            os.makedirs(directory)
            return True
        else:
            return True
    except OSError as e:
        print(f"Failed to create directory: {e}")
        return False

def savetoMongoDB(fileData):
    try:
        uri = getMangoURI()
        # Create a new client and connect to the server
        client = MongoClient(uri).SK.Census  
        # Add a custom _id column      
        fileData['_id'] = range(1, len(fileData) + 1)
        # Convert DataFrame to dictionary and insert into MongoDB
        data_dict = fileData.to_dict(orient='records')
        existing_record = client.find_one({'District code': len(fileData)})
        # print("SaveMDB: ",len(data_dict),"ExRec: ")        
        if existing_record:
            print("Data already updated in mangoDB")
            return "Success" #"Data already updated"            
        else:
            client.insert_many(data_dict)            
            return "Success"
    except Exception as e:
        return f"An error occurred: {e}"

def getfromMangoDB():
    try:
        # Connect to MongoDB    
        uri = getMangoURI()
        mangDBClient = MongoClient(uri).SK.Census
        # Fetch data from MongoDB
        mangDBData = list(mangDBClient.find())
        # print("GetMDB: ",len(mangDBData))
        return mangDBData
    except Exception as e:
        return f"An error occurred: {e}"

def getMangoURI():
    return "mongodb+srv://skrtecmail:sk123@cluster0.na93hqc.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Function to infer MySQL data type from MongoDB data
def mysqlType(value):
    if isinstance(value, int):
        return "INT"
    elif isinstance(value, float):
        return "FLOAT"
    elif isinstance(value, bool):
        return "BOOLEAN"
    elif isinstance(value, str):
        return "VARCHAR(255)"
    elif isinstance(value, ObjectId):
        return "VARCHAR(255)"
    else:
        return "TEXT"

# Function to truncate column names
def truncateColumnName(name, max_length=64):
    return name[:max_length]

def insertDatatoSQL():
    try:        
        # Get the data from mango DB
        mango_Data = getfromMangoDB()
        if mango_Data:
            # print("Mng: ",len(mango_Data))
            # Connect to SQL
            sqlDB = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            )
            mysqlDB = sqlDB.cursor(buffered=True)
            mysqlTable = 'Census'
            # Execute the SQL command to create the database only if it does not exist
            mysqlDB.execute("CREATE DATABASE IF NOT EXISTS CensusDB")
            sqlDB.commit()
            
            # Schema from the first document
            mdbRecord = mango_Data[0]    
            # print(mdbRecord['District code'], mdbRecord["District"])
            schema = {truncateColumnName(key): mysqlType(value) for key, value in mdbRecord.items()}
            # schema["_id"] = "VARCHAR(255) PRIMARY KEY"
            schema["_id"] = "INT AUTO_INCREMENT PRIMARY KEY"
            
            # Skip the primary key
            insert_schema = {key: schema[key] for key in schema if key != "_id"}
            
            # Generate CREATE TABLE query
            create_table_query = "CREATE TABLE IF NOT EXISTS `{}` (".format(mysqlTable)
            create_table_query += ", ".join(["`{}` {}".format(key, data_type) for key, data_type in schema.items()])
            create_table_query += ")"    
            # print(create_table_query)
            
            # Create table in MySQL    
            mysqlDB.execute("USE {}".format('CensusDB'))
            mysqlDB.execute(create_table_query)
            # # sqlDB.commit()
            mysqlDB.execute(f"SELECT COUNT(1) FROM CensusDB.census WHERE `District code`  = {mdbRecord['District code']}")
            idDataExists = mysqlDB.fetchone()[0]
            # print(idDataExists)
            if abs(idDataExists) > 0:
                print("Data already available in sql")
                return "Success"
            # Generate INSERT INTO query
            columns = ", ".join("`{}`".format(key) for key in insert_schema .keys())
            placeholders = ", ".join(["%s"] * len(insert_schema ))
            insert_query = "INSERT INTO `{}` ({}) VALUES ({})".format(mysqlTable, columns, placeholders)
            
            # print(insert_query)  
            # Insert data into MySQL table
            for record in mango_Data:
                values = tuple(str(record.get(col, None)) for col in insert_schema.keys())
                mysqlDB.execute(insert_query, values)
            
            # Commit the transaction
            sqlDB.commit()
            # Close the cursor and connection
            mysqlDB.close()
            sqlDB.close()   
            return "Success"         
        else:
            print("no data")
    except Exception as e:
        return f"An error occurred: {e}"

# Execute
inputPath = os.path.join(os.getcwd(),'InputFile','census_2011.xlsx') # Input file path
updateFile  = os.path.join(os.getcwd(), 'Output','Output_Census_2011.xlsx') # Output file path
sheetName = 'census_2011.csv' # excel file sheet name
# Input file header column name rename list
columnDetails = {
    'State name': 'State/UT',
    'District name': 'District',
    'Male_Literate': 'Literate_Male',
    'Female_Literate': 'Literate_Female',
    'Rural_Households': 'Households_Rural',
    'Urban_Households': 'Households_Urban',
    'Age_Group_0_29': 'Young_and_Adult',
    'Age_Group_30_49': 'Middle_Aged',
    'Age_Group_50': 'Senior_Citizen',
    'Age not stated': 'Age_Not_Stated'
}
updateData = valueChange(inputPath, updateFile,sheetName,columnDetails,'Rename',None)
if updateData == "Success":
    columnDetails = {"State/UT"} # Rename State/UT Names
    searchVal = {"and", "of"} # Specific word should be all lowercase
    updateData = valueChange(updateFile, updateFile,sheetName,columnDetails,'Capitalize',searchVal)
    if updateData == "Success":
        # Change the Telangana state name based upon districts details from the docx file
        docfile = os.path.join(os.getcwd(),'InputFile','Telangana.docx')
        docOut = getDocxData(docfile,'Andhra Pradesh', 'Telangana')        
        # Change the Ladakh state name based upon mentioned districts
        searchVal = [docOut,{'Ladakh':{'Jammu and Kashmir':['Leh','Kargil']}}]
        columnDetails = ["State/UT",'District']
        updateData = valueChange(updateFile, updateFile,sheetName,columnDetails,'District',searchVal)
        if updateData == "Success":
            updateData = valueChange(updateFile, updateFile,sheetName,None,'Data',None)            
            if updateData == "Success":   
                updateData = insertDatatoSQL()
                if updateData == "Success":
                    print(f'SQL DB Inserted: {updateData}')
                else: 
                    print(f'SQL DB update: {updateData}')
            else:
                print(f'Missing Data: {updateData}')
        else:
            print(f'District Error: {updateData}')
    else:
        print(f"Capitalize Error: {updateData}")
else:
    print(f"Rename Error: {updateData}")

