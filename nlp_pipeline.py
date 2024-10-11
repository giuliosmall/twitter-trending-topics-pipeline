from graphviz import Digraph

def create_nlp_pipeline_diagram():
    dot = Digraph(comment='NLP Pipeline')
    dot.attr(rankdir='TB', size='14,14')

    steps = [
        ('Raw Text', 'Initial tweet data: Unprocessed text from Twitter, including URLs, special characters, and mentions'),
        ('Text Cleaning', 'Remove URLs, special characters, mentions: standardize text by removing noise and non-textual elements'),
        ('Named Entity Recognition', 'Extract PERSON, ORG, GPE entities: identify and categorize key named entities using spaCy models'),
        ('Tokenization', 'Split text into individual words: break down cleaned text into separate tokens for further analysis'),
        ('Stopword Removal', 'Remove common words and single characters: filter out frequent words (e.g., "the", "is") and single letters to focus on meaningful content'),
        ('Hashtag Extraction', 'Identify and extract hashtags: separate hashtags from regular text to prioritize topic indicators'),
        ('Combine Words & Hashtags', 'Merge processed words and hashtags: create a unified list of significant terms, including both regular words and hashtags'),
        ('N-gram Generation', 'Create bigrams and trigrams: form 2-word and 3-word phrases to capture multi-word expressions and context'),
        ('TF-IDF Calculation', 'Compute term importance across documents: calculate term frequency-inverse document frequency to weigh the significance of words in the context of the entire dataset'),
        ('Filtering & Grouping', 'Remove empty lists, group by time windows: eliminate any remaining empty word lists and organize data into specified time intervals'),
        ('Ranking & Extracting Top Trends', 'Identify top N trends per time window: determine the most frequent and important terms or phrases for each time period')
    ]

    for i, (step, desc) in enumerate(steps):
        dot.node(f'step_{i}', f'{step}\n\n{desc}', shape='box', style='rounded,filled', fillcolor='lightblue')

    for i in range(len(steps) - 1):
        dot.edge(f'step_{i}', f'step_{i+1}', label=f'Step {i+1}')

    dot.render('nlp_pipeline', format='png', cleanup=True)
    print("Diagram saved as 'nlp_pipeline.png'")

create_nlp_pipeline_diagram()