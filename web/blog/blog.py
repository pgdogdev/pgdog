#
# Hacky script to build the blog.
#
import markdown
import glob
import os
import sys

script_path = os.path.abspath(sys.argv[0])
script_dir = os.path.dirname(script_path)

TITLE_REPLACEMENTS = [
    ["vs", "vs."],
    ["-", " "],
]

def build():
    with open(os.path.join(script_dir, "head.html")) as f:
        head = f.read()
    with open(os.path.join(script_dir, "footer.html")) as f:
        footer = f.read()

    def build_blog_entry(file):
        folder = os.path.basename(file).replace(".md", "")

        # Create blog entry directory
        blog_folder = os.path.join(script_dir, folder)
        os.makedirs(blog_folder, exist_ok=True)

        # Construct post title.
        title = folder
        for replacement in TITLE_REPLACEMENTS:
            title = folder.replace(replacement[0], replacement[1])
        title = title.capitalize()

        # Write blog post HTML.
        with open(file) as f:
            html = markdown.markdown(f.read())
            with open(os.path.join(blog_folder, "index.html"), "w") as f:
                f.write(head.replace("{{TITLE}}", title))
                f.write(html)
                f.write(footer)
            print(f"written {blog_folder}")

    files = glob.glob(f"{script_dir}/*.md")
    print("building blog...")
    for file in files:
        build_blog_entry(file)
    print("done")

if __name__ == "__main__":
    build()
