import os
import glob

pages_dir = "pages"
files = glob.glob(f"{pages_dir}/*.py")

count = 0
for file in files:
    with open(file, 'r') as f:
        content = f.read()
    
    if '    .stDeployButton, [data-testid="stAppDeployButton"] {display: none !important;}
    .stGithubButton, [data-testid="stToolbarActionButton"] {display: none !important;}' in content:
        new_content = content.replace('    .stDeployButton, [data-testid="stAppDeployButton"] {display: none !important;}
    .stGithubButton, [data-testid="stToolbarActionButton"] {display: none !important;}', '    .stDeployButton, [data-testid="stAppDeployButton"] {display: none !important;}
    .stGithubButton, [data-testid="stToolbarActionButton"] {display: none !important;}')
        with open(file, 'w') as f:
            f.write(new_content)
        print(f"Patched {file}")
        count += 1
    
print(f"Patched {count} files.")
