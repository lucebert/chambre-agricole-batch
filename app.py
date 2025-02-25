import asyncio
import csv
import os
import requests
from langgraph_sdk import get_client

def load_indexed_documents():
    indexed_docs = set()
    if os.path.exists('indexed_documents.csv'):
        with open('indexed_documents.csv', 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                indexed_docs.add(row['url'])
    return indexed_docs

def save_document_status(document_data, status='indexed'):
    file_exists = os.path.exists('indexed_documents.csv')
    fieldnames = ['url', 'title', 'publication_year', 'publisher', 'project_code', 'status']
    
    with open('indexed_documents.csv', 'a', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow({
            'url': document_data['url'],
            'title': document_data['title'],
            'publication_year': document_data['publication_year'],
            'publisher': document_data['publisher'],
            'project_code': document_data['project_code'],
            'status': status
        })

def call_rd_api():
    # Check if we already have indexed documents


    payload = {
            "fullText": "couverts végétaux",
            "motsCles": [],
            "enrichPath": [
                "region_complexe/Normandie"
            ],
            "organismes": [],
            "typesDonnees": ["DOCUMENT"],
            "annees": [],
            "page": 0,
            "searchSize": 2000,
            "tri": "DATE",
        }
    
    response = requests.post("https://rd-agri.fr/rest/search/getResults", json=payload, verify=False)
    data = response.json()
    return data



async def main():

    LANGGRAPH_DEPLOYMENT = "https://le-chat-bottes-08847786c41355da87302fa1e0f41f4a.us.langgraph.app"
    #LANGGRAPH_DEPLOYMENT = "http://localhost:64975"

    client = get_client(url=LANGGRAPH_DEPLOYMENT)
    assistants = await client.assistants.search(
        graph_id="indexer", metadata={"created_by": "system"}
    )
    
    indexed_docs = load_indexed_documents()
    data = call_rd_api()

    for document in data["results"]:
        thread = await client.threads.create()
        urlDocument = document.get("urlDocument")
        
        # Skip if no URL or not a PDF
        if (urlDocument is None or not urlDocument.split('?')[0].lower().endswith('.pdf')):
            print(f"Skipping document: {document.get('titre')} - Invalid URL or not a PDF")
            continue
            
        # Construct full URL if needed
        if (urlDocument.startswith('/rest/content/getFile/')):
            urlDocument = 'https://rd-agri.fr' + urlDocument
            
        # Skip if already indexed
        if urlDocument in indexed_docs:
            print(f"Skipping already indexed document: {document.get('titre')}")
            continue
            
        payload = {
            "title": document.get("titre"),
            "publication_year": document.get("anneePublication"),
            "publisher": document.get("publicateur"),
            "url": urlDocument,
            "project_code": document.get("codeProjet"),
        }

        print(f"Processing: {payload['title']}")
        
        try:
            async for chunk in client.runs.stream(
                thread_id=thread["thread_id"],
                assistant_id=assistants[0]["assistant_id"],
                input=payload,
                stream_mode="events"
            ):
                print(chunk)
            
            # Mark document as indexed after successful processing
            save_document_status(payload)
            
        except Exception as e:
            print(f"Error processing document {payload['title']}: {str(e)}")
            # Optionally save failed documents with different status
            save_document_status(payload, status='failed')
            continue
    



if __name__ == "__main__":
    asyncio.run(main())
