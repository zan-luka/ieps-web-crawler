# Information Extraction and Processing Systems - Web Crawler PA2

This project implements a text processing pipeline and information retrieval system for web content. The system extracts, processes, and indexes web content for efficient semantic search.

## Quick Start Guide

### Prerequisites

- Python 3.8+
- PostgreSQL with pgvector extension installed
- Required Python packages (see Setup section)

### Setup

1. Install required Python packages:

   ```bash
   pip install flask sqlalchemy sentence-transformers transformers torch nltk bs4 psycopg2-binary requests pgvector lxml
   ```

2. Download required NLTK data:

   ```python
   import nltk
   nltk.download('punkt')
   ```

3. Ensure the PostgreSQL database is running with pgvector extension

### Starting the System

1. **Start the Flask server**:

   ```bash
   cd pa2/server
   python main.py
   ```

   The server will start on http://localhost:5000

2. **Process HTML content** (run after server is started):

   ```bash
   cd pa2/client
   python preprocesser.py
   ```

   This will:

   - Fetch HTML pages from the database
   - Clean and structure the HTML content
   - Extract article text, comments, and metadata
   - Generate embeddings using LaBSE/SloBERTa models
   - Store processed content and embeddings back to the database

3. **Run the demo** for information retrieval:
   ```bash
   cd pa2/client
   python demo.py
   ```
   The server will start on http://localhost:5001

### Using the Demo

The `demo.py` script provides an interactive interface for searching through the processed content:

1. When prompted, enter your search query in Slovenian or English
2. The system will return the top k most semantically similar text segments from the database
3. Results include:
   - Similarity score
   - Text segment
   - Segment type (title, paragraph, comment)
