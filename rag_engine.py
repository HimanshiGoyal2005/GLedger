
import os
from pathlib import Path
from typing import List, Dict, Any, Optional
import json


try:
    from langchain.text_splitter import RecursiveCharacterTextSplitter
    from langchain_community.document_loaders import TextLoader, DirectoryLoader
    from langchain_community.vectorstores import Chroma
    from langchain_openai import OpenAIEmbeddings
    from langchain.schema import Document
    LANGCHAIN_AVAILABLE = True
except ImportError:
    LANGCHAIN_AVAILABLE = False

import pathway as pw

DOCUMENTS_DIR = Path(__file__).parent / "documents"
CHROMA_PERSIST_DIR = "./chroma_db"
EMBEDDING_MODEL = "text-embedding-ada-002"

class ComplianceDocumentStore:
    """Manages compliance documents for RAG"""
    
    def __init__(self, documents_dir: Path = DOCUMENTS_DIR):
        self.documents_dir = documents_dir
        self.documents: List[Document] = []
        self.vectorstore = None
        
    def load_documents(self) -> List[Document]:
        """Load all documents from the documents directory"""
        if not LANGCHAIN_AVAILABLE:
            print("Warning: LangChain not available. Using simple text search.")
            return self._load_simple()
        
        documents = []
        for file_path in self.documents_dir.glob("*"):
            if file_path.is_file() and not file_path.name.startswith('.'):
                try:
                    loader = TextLoader(str(file_path))
                    docs = loader.load()
                    for doc in docs:
                        doc.metadata = {
                            "source": file_path.name,
                            "path": str(file_path)
                        }
                    documents.extend(docs)
                    print(f"Loaded: {file_path.name}")
                except Exception as e:
                    print(f"Error loading {file_path.name}: {e}")
        
        self.documents = documents
        return documents
    
    def _load_simple(self) -> List[Document]:
        """Simple document loading without LangChain"""
        documents = []
        for file_path in self.documents_dir.glob("*"):
            if file_path.is_file():
                try:
                    content = file_path.read_text(encoding='utf-8')
                    doc = Document(
                        page_content=content,
                        metadata={"source": file_path.name, "path": str(file_path)}
                    )
                    documents.append(doc)
                    print(f"Loaded: {file_path.name}")
                except Exception as e:
                    print(f"Error loading {file_path.name}: {e}")
        
        self.documents = documents
        return documents
    
    def split_documents(self, chunk_size: int = 1000, chunk_overlap: int = 200) -> List[Document]:
        """Split documents into chunks"""
        if not self.documents:
            self.load_documents()
        
        if not LANGCHAIN_AVAILABLE:
            # Simple chunking without LangChain
            chunks = []
            for doc in self.documents:
                content = doc.page_content
                for i in range(0, len(content), chunk_size - chunk_overlap):
                    chunk = content[i:i + chunk_size]
                    chunks.append(Document(
                        page_content=chunk,
                        metadata=doc.metadata
                    ))
            return chunks
        
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap
        )
        return text_splitter.split_documents(self.documents)
    
    def create_vectorstore(self, embeddings=None) -> Any:
        """Create vector store from documents"""
        if not LANGCHAIN_AVAILABLE:
            print("Warning: Cannot create vectorstore without LangChain")
            return None
        
        chunks = self.split_documents()
        
        if embeddings is None:
            # Try to get OpenAI embeddings
            api_key = os.environ.get("OPENAI_API_KEY")
            if api_key:
                embeddings = OpenAIEmbeddings(model=EMBEDDING_MODEL)
            else:
                print("Warning: OPENAI_API_KEY not set. Using dummy embeddings.")
                return None
        
        # Create Chroma vectorstore
        self.vectorstore = Chroma.from_documents(
            documents=chunks,
            embedding=embeddings,
            persist_directory=CHROMA_PERSIST_DIR
        )
        
        print(f"Created vectorstore with {len(chunks)} chunks")
        return self.vectorstore
    
    def similarity_search(self, query: str, k: int = 4) -> List[Document]:
        """Search for similar documents"""
        if self.vectorstore:
            return self.vectorstore.similarity_search(query, k=k)
        elif self.documents:
            # Simple keyword search fallback
            return self._simple_search(query, k)
        else:
            return []
    
    def _simple_search(self, query: str, k: int = 4) -> List[Document]:
        """Simple keyword-based search"""
        query_lower = query.lower()
        results = []
        
        for doc in self.documents:
            # Count keyword matches
            score = sum(1 for word in query_lower.split() if word in doc.page_content.lower())
            if score > 0:
                results.append((score, doc))
        
        # Sort by score and return top k
        results.sort(key=lambda x: x[0], reverse=True)
        return [doc for _, doc in results[:k]]
    
    def get_context(self, query: str, k: int = 4) -> str:
        """Get context string from search results"""
        docs = self.similarity_search(query, k=k)
        
        context_parts = []
        for i, doc in enumerate(docs, 1):
            source = doc.metadata.get('source', 'Unknown')
            context_parts.append(f"[Source {i}: {source}]\n{doc.page_content}")
        
        return "\n\n".join(context_parts)

InputSchema = pw.schema_builder(
    columns={
        "query": pw.column_definition(dtype=str),
    }
)


@pw.table
class QueryTable:
    query: str


query_input = QueryTable(
    pw.io.stdin.read(
        schema=InputSchema,
        format="json",
    )
)


doc_store = ComplianceDocumentStore()
doc_store.load_documents()

def process_query(query_str: str) -> dict:
    """Process a query and return context"""
    context = doc_store.get_context(query_str)
    
    return {
        "query": query_str,
        "context": context,
        "num_sources": len(context.split("[Source")) - 1 if context else 0
    }


queries = query_input.select(
    query=pw.this.query,
    processed=pw.apply(process_query, pw.this.query),
)

pw.io.stdout.write(
    queries.select(
        pw.this.query,
        context=pw.this.processed["context"],
        num_sources=pw.this.processed["num_sources"],
    ),
    format="json",
)

if __name__ == "__main__":
    import argparse
    import sys
    
    parser = argparse.ArgumentParser(description="GreenLedger RAG Engine")
    parser.add_argument("--rebuild", action="store_true",
                        help="Rebuild vectorstore")
    parser.add_argument("--query", type=str, default=None,
                        help="Run a single query")
    args = parser.parse_args()
    
  
    doc_store = ComplianceDocumentStore()
    
    if args.rebuild:
        print("Rebuilding vectorstore...")
        doc_store.load_documents()
        doc_store.create_vectorstore()
    elif args.query:
        
        doc_store.load_documents()
        context = doc_store.get_context(args.query)
        print(f"Query: {args.query}")
        print(f"\nContext:\n{context}")
    else:
   
        print("Starting GreenLedger RAG Engine...")
        print(f"Documents directory: {DOCUMENTS_DIR}")
        
        if not LANGCHAIN_AVAILABLE:
            print("Warning: Running in basic mode without LangChain")
        
        pw.run()
