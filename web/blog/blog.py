from posix import mkdir
import markdown
import glob
import os
import sys

script_path = os.path.abspath(sys.argv[0])
script_dir = os.path.dirname(script_path)

with open(os.path.join(script_dir, "head.html")) as f:
    head = f.read()
with open(os.path.join(script_dir, "footer.html")) as f:
    footer = f.read()

def setup_blog_entry(file):
    folder = os.path.basename(file).replace(".md", "")
    blog_folder = os.path.join(script_dir, folder)
    os.makedirs(blog_folder, exist_ok=True)
    with open(file) as f:
        text = f.read()
        html = markdown.markdown(text)
        with open(os.path.join(blog_folder, "index.html"), "w") as f:
            f.write(head)
            f.write(html)
            f.write(footer)



files = glob.glob(f"{script_dir}/*.md")


for file in files:
    setup_blog_entry(file)
