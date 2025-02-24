#
# Hacky script to build the blog.
#
import markdown
import glob
import os
from time import sleep
import sys

script_path = os.path.abspath(sys.argv[0])
script_dir = os.path.dirname(script_path)

with open(os.path.join(script_dir, "head.html")) as f:
    head = f.read()
with open(os.path.join(script_dir, "footer.html")) as f:
    footer = f.read()

def build_blog_entry(file):
    folder = os.path.basename(file).replace(".md", "")
    blog_folder = os.path.join(script_dir, folder)
    title = folder.replace("-", " ").replace("vs", "vs.").capitalize()
    os.makedirs(blog_folder, exist_ok=True)
    with open(file) as f:
        text = f.read()
        html = markdown.markdown(text)
        with open(os.path.join(blog_folder, "index.html"), "w") as f:
            f.write(head.replace("{{TITLE}}", title))
            f.write(html)
            f.write(footer)
        print(f"written {blog_folder}")

files = glob.glob(f"{script_dir}/*.md")

while True:
    print("building blog...")
    for file in files:
        build_blog_entry(file)
    print("done")
    sleep(2)
