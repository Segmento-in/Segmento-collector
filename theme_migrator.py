import os
import re

directory = r"C:\Users\HP\OneDrive\Desktop\PROJECTS\Segmento_Collector\frontend\templates"

replacements = {
    # Text
    r'\btext-white\b(?! dark:)': 'text-slate-900 dark:text-white',
    r'\btext-slate-300\b(?! dark:)': 'text-slate-600 dark:text-slate-300',
    r'\btext-slate-400\b(?! dark:)': 'text-slate-500 dark:text-slate-400',
    r'\btext-slate-200\b(?! dark:)': 'text-slate-700 dark:text-slate-200',

    # Backgrounds
    r'\bbg-slate-900\b(?! dark:)': 'bg-white dark:bg-slate-900',
    r'\bbg-slate-800\b(?! dark:)': 'bg-slate-100 dark:bg-slate-800',
    r'\bbg-slate-950\b(?! dark:)': 'bg-slate-50 dark:bg-slate-950',
    
    # Borders
    r'\bborder-slate-700\b(?! dark:)': 'border-slate-300 dark:border-slate-700',
    r'\bborder-slate-800\b(?! dark:)': 'border-slate-200 dark:border-slate-800',
    r'\bborder-slate-600\b(?! dark:)': 'border-slate-300 dark:border-slate-600',
    r'\bborder-white/10\b(?! dark:)': 'border-slate-200 dark:border-white/10',
    r'\bborder-white/5\b(?! dark:)': 'border-slate-200 dark:border-white/5',
}

def process_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    original = content
    for pattern, new_val in replacements.items():
        # we don't want to replace if the string already has the dark version just ahead
        # e.g., if it's already "text-slate-900 dark:text-white", we don't replace text-white.
        # It's a bit tricky. We'll use a regex that asserts dark: is NOT right next to it or behind it.
        # Better: just do a simple replace, but avoid messing up existing dark: classes.
        pass
    
    # Actually, a simpler replace using string methods but checking avoiding double-ups:
    # We can use re.sub iteratively.
    for p_string, rep in replacements.items():
        # Only replace if the target is not preceded by 'dark:' and not already part of the replacement pair
        # e.g. text-white -> text-slate-900 dark:text-white
        # but if the text is 'dark:text-white', ignore. 
        # if the text is 'text-slate-900 dark:text-white', ignore 'text-white' part.
        
        # negative lookbehind for dark:
        regex = r'(?<!dark:)' + p_string
        
        # to avoid duplicating if we run multiple times: 
        # if the file already has rep (like 'text-slate-900 dark:text-white'), we temporarily mask it.
        # But honestly, since I've only done index and base so far, masking is easier.
        content = re.sub(regex, rep, content)

    # Some cleanup for things already replaced manually.
    content = content.replace('text-slate-900 dark:text-slate-900 dark:text-white', 'text-slate-900 dark:text-white')
    content = content.replace('bg-white dark:bg-white dark:bg-slate-900', 'bg-white dark:bg-slate-900')
    content = content.replace('bg-slate-50 dark:bg-slate-50 dark:bg-slate-950', 'bg-slate-50 dark:bg-slate-950')
    
    if original != content:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"Updated: {filepath}")

for root, dirs, files in os.walk(directory):
    for file in files:
        if file.endswith('.html'):
            process_file(os.path.join(root, file))

print("Done")
