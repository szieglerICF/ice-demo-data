import cmd
import logging
from dotenv import load_dotenv
import json 

load_dotenv()
import os


import colorama
from ChatToJson import RAGToText, RAGFileCollection

logging.basicConfig(filename='log.txt', filemode='a', level=logging.INFO)

rfc = RAGFileCollection(file_listing="/Users/sziegler/Documents/GitHub/proprag/bd/prop_file_list_chroma.txt")
files = rfc.filter("ice demo")
chatter_rag = RAGToText(rfc.filter("ice demo"),"/Users/sziegler/Documents/GitHub/proprag/cli/bd/chroma_db")

class SearchRAG(cmd.Cmd):
    intro = """
    
|---------------------------------------------------------
| *   *   *   *   *   * #################################|
|   *   *   *   *   *                                    |
| *   *   *   *   *   *                                  |
|   *   *   *   *   *   #################################|
| *   *   *   *   *   *                                  |
|   *   *   *   *   *                                    |
| *   *   *   *   *   * #################################|
|   *   *   *   *   *                                    |
| *   *   *   *   *   *                                  |
|########################################################|
|                                                        |
|                                                        |
|########################################################|
|                                                        |
|                                                        |
|########################################################|
|                                                        |
|                                                        |
|########################################################|
|---------------------------------------------------------
    
    Welcome to the RAG search shell.   Type help or ? to list commands.\n """
    prompt = 'ICEBot> '

    def do_c(self, arg):
        'Chat about a lead'
        logging.info(f"Searching for {arg}")
        prompt = f"Chat about {arg} {{context}}"
        results = chatter_rag.get(arg)
        print(results)


    def do_EOF(self, line):
        return True
    
    def do_exit(self, arg):
        return True
    
    def do_quit(self, arg):
        return True
    
    def do_q(self, arg):
        return True
    
if __name__ == '__main__':
    SearchRAG().cmdloop()