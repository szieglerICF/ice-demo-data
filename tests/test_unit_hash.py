import unittest
from unittest.mock import MagicMock, Mock, PropertyMock, patch

from datasketch import MinHash
from icecream import ic


class Create(unittest.TestCase):
    def test_should_do_hello_world(self):
        hash = TextFuzzyMinhash().create_hash("Hello World")
        self.assertNotEqual(hash, "")
    
    def test_should_do_hello_world_space(self):
        hash = TextFuzzyMinhash().create_hash("HelloWorld ")
        self.assertNotEqual(hash, "")

    def test_should_do_steve(self):
        hash = TextFuzzyMinhash().create_hash("steveziegler")
        ic(hash)
        ic(hash.digest())
        self.assertNotEqual(hash, "")

class Compare(unittest.TestCase):
    def test_should_do_simple_hello_world(self):
        tfh = TextFuzzyMinhash()
        hash1 = tfh.create_hash("Hello World ")
        hash2 = tfh.create_hash("Hello World ")
        self.assertGreater(tfh.compare(hash1, hash2), .8)
    

    def test_should_do_completely_different(self):
        tfh = TextFuzzyMinhash()
        hash1 = tfh.create_hash("Hello World")
        hash2 = tfh.create_hash("apple pear!")
        self.assertLess(tfh.compare(hash1, hash2), .2)

    def test_should_do_completely_different_long(self):
        tfh = TextFuzzyMinhash()
        hash1 = tfh.create_hash("The quick brown fox jumped over the lazy dog.")
        hash2 = tfh.create_hash("The Washington Commanders made it to NCF Cha.")
        self.assertLess(tfh.compare(hash1, hash2), .2)

    def test_should_do_completely_different_ice(self):
        tfh = TextFuzzyMinhash()
        hash1 = tfh.create_hash("John Doe, Jan 1, 2022")
        hash2 = tfh.create_hash("The Washington Commanders made it to NCF Cha.")
        self.assertLess(tfh.compare(hash1, hash2), .2)


class TextFuzzyMinhash():
    def create_hash(self, text, num_perm=128):
        """Generate a MinHash signature for a given text."""
        minhash = MinHash(num_perm=num_perm)
        for word in text.split():
            minhash.update(word.encode('utf8'))
        return minhash

    def compare(self, hash1, hash2):
        return hash1.jaccard(hash2)


# def get_minhash(text, num_perm=128):
#     """Generate a MinHash signature for a given text."""
#     minhash = MinHash(num_perm=num_perm)
#     for word in text.split():
#         minhash.update(word.encode('utf8'))
#     return minhash

# # Example texts
# text1 = "This is a sample document for fuzzy matching."
# text2 = "This is a sample doc for fuzzy match."

# # Create MinHash signatures
# minhash1 = get_minhash(text1)
# minhash2 = get_minhash(text2)

# # Compute Jaccard similarity
# similarity = minhash1.jaccard(minhash2)
# print(f"MinHash Jaccard Similarity: {similarity:.2f}")




class OldTextFuzzyHash:
    # given a text string, convert each letter to its corresponding ASCII value, then determine the average of all the values
    # then, for each letter, determine if the ASCII value is above or below the average value
    # if it is above, replace the letter with a 1, if it is below, replace the letter with a 0
    # return the resulting string of 1s and 0s
    def create_hash(self, text: str) -> str:
        ascii_values = [ord(c) for c in text]
        average_ascii = sum(ascii_values) / len(ascii_values)
        return "".join(["1" if a >= average_ascii else "0" for a in ascii_values])

    def compare(self, hash1: str, hash2: str) -> float:
        if len(hash1) != len(hash2):
            raise ValueError("Hashes must be the same length")
        ic(hash1)
        ic(hash2)
        return sum([1 if hash1[i] == hash2[i] else 0 for i in range(len(hash1))]) / len(hash1)


if __name__ == "__main__":
    unittest.main()