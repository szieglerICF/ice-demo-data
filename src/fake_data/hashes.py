import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


class CosineSimilarityCalculator:
    def __init__(self):
        self.vectorizer = TfidfVectorizer()

    def vectorize_text(self, text: str) -> np.ndarray:
        """Converts text into a TF-IDF vector (limited to 2000 chars)."""
        tfidf_matrix = self.vectorizer.fit_transform([text[:2000]])
        return tfidf_matrix.toarray()[0]  # Convert sparse matrix to array

    def compute_cosine_similarity(self, text1: str, text2: str) -> tuple:
        """Computes cosine similarity and returns both raw and percentage format."""
        tfidf_matrix = self.vectorizer.fit_transform([text1[:2000], text2[:2000]])
        vector1 = tfidf_matrix.toarray()[0]
        vector2 = tfidf_matrix.toarray()[1]
        similarity = cosine_similarity([vector1], [vector2])[0][0]
        similarity_percentage = similarity * 100  # Convert to percentage
        return similarity, f"{similarity_percentage:.2f}%"

# Example usage:
similarity_calculator = CosineSimilarityCalculator()


text1 = 'Stephen Ziegler 6/12/75  123-45-6789'

text2 = 'Steve Ziegler 6/12/75 123-45-6789'


raw_similarity, similarity_percentage = similarity_calculator.compute_cosine_similarity(text1.lower(), text2.lower())

print("Raw Cosine Similarity:", raw_similarity)
print("Similarity Percentage:", similarity_percentage)
