from github import Github
import os

# GitHub credentials
TOKEN = 'private'
REPO_NAME = 'ZafraFarhan/Leptospirosis'  # Repository name
FILE_PATH = '/home/naethree/Users/naethree/airflow/dags/model/Sri Lanka.pkl'
COMMIT_MESSAGE = 'Add Sri Lanka.pkl model file'
BRANCH_NAME = 'main'  # Branch where you want to upload the file

# Initialize GitHub client
g = Github(TOKEN)
repo = g.get_repo(REPO_NAME)

# Verify if the file exists
if not os.path.exists(FILE_PATH):
    raise FileNotFoundError(f"File not found: {FILE_PATH}")

# Read the file content
with open(FILE_PATH, 'rb') as file:
    content = file.read()

# Get the file name from the path
file_name = os.path.basename(FILE_PATH)

try:
    # Check if the file already exists in the repository
    try:
        existing_file = repo.get_contents(file_name, ref=BRANCH_NAME)
        # Delete the existing file
        repo.delete_file(existing_file.path, f"Delete {file_name} before upload", existing_file.sha, branch=BRANCH_NAME)
        print(f"Existing file {file_name} deleted successfully.")
    except Exception as e:
        # File does not exist, ignore
        print(f"No existing file found: {e}")

    # Upload the new file
    repo.create_file(file_name, COMMIT_MESSAGE, content, branch=BRANCH_NAME)
    print("File uploaded successfully.")

except Exception as e:
    print(f"Error: {e}")
