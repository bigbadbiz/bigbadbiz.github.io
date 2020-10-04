#!/usr/bin/python
import json
from bson import json_util
from pymongo import MongoClient
import pprint
import datetime

# Database connection and selection
connection = MongoClient('localhost', 27017)
db = connection['market']
collection = db['stocks']

# Create new document in data base
def create_document(document):
	try:
		result=collection.insert_one(document)
		print("The Document has been successfully created!\n")
	except Exception as ve:
		abort(400, str(ve))
	return result

# Reads document based of predetermined query input
def read_document(query):
	try:
		result=collection.find_one(query)
		print(type(result))
		print(result)
	except Exception as ve:
		abort(400, str(ve))
	return result

# Update document based of predetermined query input
def update_document(criteria, document):
	try:
		collection.update_one(criteria,{"$set" : document})
		result = collection.find(criteria)
		print("The Document has successfully updated with your update information!")
	except Exception as ve:
		abort(400, str(ve))
	return result 


# Deletes document based of predetermined query input
def delete_document(document):
	try:
		result=collection.delete_one(document)
		print("The Document you have selected has been Deleted succesfully!")
	except Exception as ve:
		abort(400, str(ve))
	return result


# Main function to run opperations and call functions
def main():
	date = datetime.datetime.now()
	i = 0

	# Loop to run all services
	while i < 1:
		print("The following operations are available:")
		print(" : 1 :   Create/input a Document")
		print(" : 2 :   Read/Find a Document")
		print(" : 3 :   Update/Change a Document")
		print(" : 4 :   Delete/remove a Document")
		print(" : 5 :   Run an Aggregation Pipeline")
		print(" : 6 :   Exit Service Program")

		operation = input("From the list above, please type the number of the operation you would like to perform: ")

		# Calls Create Service
		if operation == 1:
			print("You have selected to Create/Input a Document\n")
			input_info = input("Please enter a Document that you would like to CREATE: \n")
			create_document(input_info)

		# Calls Read/Find Service
		elif operation == 2:
			print("You have selected to Read/Find a Document\n")
			search_info = input("Please enter a Document that you would like to RETRIEVE: \n")
			read_document(search_info)

		# Calls Update/Change Service
		elif operation == 3:
			print("You have selected to Update/Change a Document\n")
			doc_info = input("Please enter Document you intend to UPDATE\n")
			criteria_info = input("Please enter criteria of Document that you would like to update\n")
			update_document(doc_info, criteria_info)
			print("Below is the document you have selected to UPDATE with new updates.\n")
			read_document(doc_info)

		# Calls Delete/Remove Service
		elif operation == 4:
			print("You have selected to Delete/Remove a Document\n")
			delete_info = input("Please enter a Document that you would like to DELETE: \n")
			delete_document(delete_info)

		# Calls Aggregation Pipeline Service
		elif operation == 5:
			print("You have selected to Run an Aggregation Pipeline\n")
			print("The following Aggregation Pipelines are available:")
			print(" : 1 :   Total Volume of Shares by Country for a Specified Sector")
			print(" : 2 :   Total Outstanding Shares by Industry for a Country (with Sort Option)")
			print(" : 3 :   Return on Investment per Industry within a Sector for a Country")
			print(" : 4 :   Average Sales Growth in Last Five Years per.. (User Options)")
			print(" : 5 :   To Return to Main Service Screen")

			operationPL = input("Please type the number of the Aggregation Pipeline that you would like to run: \n")

			# Calls Aggregation Pipeline One
			if operationPL == 1:
				print("You have selected Pipeline ONE, \"Total Volume of Shares by Country for a Sector\" \n")
				print("Below is the list of Current Available Sectors \n")

				#Pipeline to Display Available Sectors
				pipelineSEC = [{"$group": {"_id": "$Sector"}}, {"$sort": {"Sector" : 1}}]
				pprint.pprint(list(db.stocks.aggregate(pipelineSEC)))				
				querySect = input("Please enter the Sector that we will input into the pipeline: \n")

				#Aggregates Final Pipleline
				pipelineFIN = [{"$match": {"Sector" : querySect}}, {"$group": {"_id": "$Country", "Total Volume of Shares": {"$sum": "$Volume"}}}, {"$sort": {"Total Volume of Shares": -1}}]
				pprint.pprint(list(db.stocks.aggregate(pipelineFIN)))
				print("Aggregation Pipeline Completed Successfully!")
				print("\n")

			# Calls Aggregation Pipeline Two
			elif operationPL == 2:
				print("You have selected Pipeline TWO, \"Total Outstanding Shares by Industry for a Country\" \n")
				print("Below is the list of Current Available Countries \n")

				#Pipeline to Display Available Countries
				pipelineCNT = [{"$group": {"_id": "$Country"}}, {"$sort": {"Country" : 1}}]
				pprint.pprint(list(db.stocks.aggregate(pipelineCNT)))				
				queryCNT = input("Please enter the Country that we will input into the pipeline: \n")

				#Get Sort Preference
				print("Below are your two Options for Sorting.")
				print(" : 1 :   Sort Total Outstanding Shares in Ascending Order")
				print(" : 2 :   Sort Total Outstanding Shares in Descending Order")
				SortInput = input("Please enter Selection for Sorting: \n")

				#Aggregates Final Pipleline
				pipelineFIN2 = [{"$match": {"Country" : queryCNT}}, {"$group": {"_id": "$Industry", "Total Outstanding Shares": {"$sum": "$Shares Outstanding"}}}, {"$sort": {"Total Outstanding Shares": SortInput}}]
				if SortInput == 2:
					pipelineFIN2 = [{"$match": {"Country" : queryCNT}}, {"$group": {"_id": "$Industry", "Total Outstanding Shares": {"$sum": "$Shares Outstanding"}}}, {"$sort": {"Total Outstanding Shares": -1}}]
				pprint.pprint(list(db.stocks.aggregate(pipelineFIN2)))
				print("Aggregation Completed Successfully!")
				print("\n")

			# Calls Aggregation Pipeline Three
			elif operationPL == 3:
				print("You have selected Pipeline THREE, \"Return on Investment per Industry withina Sector for a Country\" \n")
				
				#Pipeline to Display Available Countries
				print("Below is the list of Current Available Countries \n")
				pipelineCNT2 = [{"$group": {"_id": "$Country"}}, {"$sort": {"Country" : 1}}]
				pprint.pprint(list(db.stocks.aggregate(pipelineCNT2)))				
				queryCNT2 = input("Please enter the Country that we will input into the pipeline: \n")

				#Pipeline to Display Available Sectors within that Country
				print("Below is the list of Current Sectors available within the country of " + queryCNT2 + " \n")
				pipelineQueryCNT3 = [{"$match": {"Country": "USA"}}, {"$group": {"_id": "$Sector"}}]
				pprint.pprint(list(db.stocks.aggregate(pipelineQueryCNT3)))				
				querySEC2 = input("Please enter the Sector that we will input into the pipeline: \n")

				#Aggregates Final Pipleline
				print("You have selected to aggregate results for Return on Investment in the " + querySEC2 + " Sector for the Country of " + queryCNT2 + "\n")
				pipelineFIN = [{"$match": {"Country": queryCNT2, "Sector": querySEC2}}, {"$group": {"_id": "$Industry", "ROI": {"$sum": "$Return on Investment"}}}, {"$sort": {"ROI": -1}}]
				pprint.pprint(list(db.stocks.aggregate(pipelineFIN)))
				print("Aggregation Pipeline Completed Successfully!")
				print("\n")

			# Calls Aggregation Pipeline Four
			elif operationPL == 4:
				print("You have selected to aggregate the \"Average Sales Growth in Last Five Years\" per an Option \n")

				#Get User Criteria
				print("Please Select from the following options: \n")
				print(" : 1 :   Country")
				print(" : 2 :   Industry")
				print(" : 3 :   Sector")
				print(" : 4 :   Company")
				queryUSERSEL = input("Please enter the Sector that we will input into the pipeline: \n")

				if queryUSERSEL == 1:
					critCal = "$Country"
				elif queryUSERSEL == 2:
					critCal = "$Industry"
				elif queryUSERSEL == 3:
					critCal = "$Sector"
				elif queryUSERSEL == 4:
					critCal = "$Company"

				#Get User Limit
				userLimit = input("Please enter the number of Documents to return from 1 - 100: \n")
				if userLimit > 100:
					userLimit == 100
				elif userLimit < 1:
					userLimit == 1

				pipeline = [{"$group": {"_id": critCal, "Average Sales Growth in Last Five Years": {"$avg": "$Sales growth past 5 years"}}}, {"$sort": {"Average Sales Growth in Last Five Years": -1}}, {"$limit" :userLimit}]
				pprint.pprint(list(db.stocks.aggregate(pipeline)))
				print("The Aggregation Pipeline Completed Successfully!")
				print("\n")
			
			# Ends Aggregation Pipeline
			else:
				print("You have made an invalid selection or have selected to Return to the Main Screen")
				print("Please Run an Aggregation Pipeline again if desired\n")

		# Ends Service Loop
		elif operation == 6:
			print("\n")
			break

		# Returns Service Loop
		else:
			print("You have made an invalid selection, please try again\n")
			print("\n")
	print("You have selected to exit the Service Program is, Thank you!   Goodbye! \n")

# Runs python script as a program
if __name__ == '__main__':
	main()
