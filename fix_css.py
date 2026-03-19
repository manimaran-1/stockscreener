import os
import glob

pages_dir = "pages"
files = glob.glob(f"{pages_dir}/*.py")

count = 0
for file in files:
    with open(file, 'r') as f:
        content = f.read()
    
    if '.stDeployButton {display:none;}' in content:
        new_content = content.replace('.stDeployButton {display:none;}', '.stDeployButton {display:none;}')
        with open(file, 'w') as f:
            f.write(new_content)
        print(f"Patched {file}")
        count += 1
    
print(f"Patched {count} files.")
