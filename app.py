# File: app.py

import streamlit as st
import boto3
from sqlalchemy import create_engine, MetaData, Table, Column, String

# AWS Configuration
S3_BUCKET = "fastq-uploads"

# Database Configuration
DB_USER = "postgre"
DB_PASSWORD = "nathanisfat88"
DB_HOST = "fastq-metadata-db.cb6qo2q20bha.us-east-1.rds.amazonaws.com"
DB_NAME = "fastq-metadata-db"

# Connect to S3
s3 = boto3.client('s3')

# Connect to Database
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")

# Define metadata and create table
metadata = MetaData()
samples = Table(
    'samples', metadata,
    Column('sample_id', String, primary_key=True),
    Column('experiment_date', String),
    Column('description', String),
    Column('file_name', String)
)

# Ensure the table exists at the start of the app
metadata.create_all(engine)

# Streamlit App
st.title("FASTQ File Uploader with Metadata")

# File Upload Section
st.subheader("Upload .fastq File")
uploaded_file = st.file_uploader("Choose a .fastq file", type=["fastq"])

# Metadata Input Section
st.subheader("Input Metadata")
sample_id = st.text_input("Sample ID")
experiment_date = st.date_input("Experiment Date")
description = st.text_area("Description")

# Upload Button
if st.button("Upload"):
    if uploaded_file is not None and sample_id:
        try:
            # Upload File to S3
            file_name = uploaded_file.name
            file_content = uploaded_file.getvalue()
            s3.put_object(Bucket=S3_BUCKET, Key=file_name, Body=file_content)
            st.success(f"File {file_name} uploaded to S3 bucket {S3_BUCKET}.")

            # Save Metadata to Database
            try:
                with engine.connect() as conn:
                    result = conn.execute(samples.insert(), {
                        'sample_id': sample_id,
                        'experiment_date': experiment_date.isoformat(),
                        'description': description,
                        'file_name': file_name
                    })
                    conn.commit()
                    print(f"Inserted rows: {result.rowcount}")  # Log the row count
                    st.success(f"Metadata for {file_name} inserted successfully.")
            except Exception as e:
                st.error(f"Database error: {e}")
                print(f"Database error: {e}")
        except Exception as e:
            st.error(f"An error occurred: {e}")
    else:
        st.error("Please upload a file and provide a sample ID.")

# To run: streamlit run app.py --server.port 8080
