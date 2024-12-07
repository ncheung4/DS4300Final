import boto3
import os
import gzip

# Initialize S3 client
s3 = boto3.client('s3')

# QC Script: Example to Count Reads and Calculate Average Quality Score in a FASTQ File
def quality_control(input_file_path):
    """
    Basic QC: Count the number of reads and calculate the average quality score in a FASTQ file.
    """
    read_count = 0
    total_quality_score = 0
    
    try:
        with gzip.open(input_file_path, 'rt') if input_file_path.endswith('.gz') else open(input_file_path, 'r') as f:
            for i, line in enumerate(f):
                line = line.strip()
                if i % 4 == 0:  # Sequence header
                    read_count += 1
                elif i % 4 == 3:  # Quality score line
                    if len(line) == 0:
                        raise ValueError(f"Empty quality score line at read {read_count}")
                    
                    quality_scores = [ord(char) - 33 for char in line]  # Convert ASCII to Phred quality scores
                    total_quality_score += sum(quality_scores)
        
        # Avoid division by zero
        average_quality_score = (
            total_quality_score / (read_count * len(quality_scores))
            if read_count > 0 and len(quality_scores) > 0 else 0
        )
        return f"Total reads: {read_count}\nAverage quality score: {average_quality_score:.2f}"
    except Exception as e:
        return f"Error processing file: {e}"

def lambda_handler(event, context):
    # Log event for debugging
    print("Event: ", event)
    
    # Get bucket and object key from the event
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    source_key = event['Records'][0]['s3']['object']['key']
    destination_bucket = "fastq-qc-results"
    
    try:
        # Download the file from S3
        local_file_path = f"/tmp/{os.path.basename(source_key)}"
        s3.download_file(source_bucket, source_key, local_file_path)
        print(f"Downloaded {source_key} from {source_bucket}")

        # Run the QC script
        qc_output = quality_control(local_file_path)
        print("QC Output: ", qc_output)

        # Upload the QC result to the destination bucket
        result_key = f"{os.path.splitext(source_key)[0]}_qc.txt"
        s3.put_object(Bucket=destination_bucket, Key=result_key, Body=qc_output)
        print(f"QC result uploaded to {destination_bucket}/{result_key}")

        return {
            'statusCode': 200,
            'body': f"QC result uploaded to {destination_bucket}/{result_key}"
        }
    except Exception as e:
        print("Error: ", e)
        return {
            'statusCode': 500,
            'body': f"Error processing file: {e}"
        }
