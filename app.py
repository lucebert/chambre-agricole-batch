import asyncio

import requests
from langgraph_sdk import get_client


def call_rd_api():
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

    LANGGRAPH_DEPLOYMENT = "https://chambre-agricole-chatbot-686407044d7f59d29a1e494685864177.us.langgraph.app/"
    #LANGGRAPH_DEPLOYMENT = "http://localhost:64975"

    client = get_client(url=LANGGRAPH_DEPLOYMENT)
    assistants = await client.assistants.search(
        graph_id="indexer", metadata={"created_by": "system"}
    )
    
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
            
        payload = {
            "title": document.get("titre"),
            "publication_year": document.get("anneePublication"),
            "publisher": document.get("publicateur"),
            "url": urlDocument,
            "project_code": document.get("codeProjet"),
        }

        print(payload)
        
        try:
            async for chunk in client.runs.stream(
                thread_id=thread["thread_id"],
                assistant_id=assistants[0]["assistant_id"],
                input=payload,
                stream_mode="events"
            ):
                print(chunk)
        except Exception as e:
            print(f"Error processing document {payload['title']}: {str(e)}")
            continue
    



if __name__ == "__main__":
    asyncio.run(main())
