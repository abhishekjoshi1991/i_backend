import os
import sys
project_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(project_path)
import pandas as pd
from sqlalchemy import text
from urllib.parse import unquote
from database import get_db_engine, get_db_engine_2

class GetFeedbackDataCopy():
    def __init__(self):
        self.engine = get_db_engine_2()

    # def get_title_identifier_text_df(self):
    #     with self.engine.connect() as connection:
    #         query = """
    #                 SELECT wp.id, identifier, title, text FROM wiki_pages wp
    #                 left join wiki_contents on wp.id = wiki_contents.page_id
    #                 left join wikis on wp.wiki_id = wikis.id
    #                 left join projects on wikis.project_id = projects.id
    #         """
    #         records = connection.execute(text(query)).fetchall()
    #         df = pd.DataFrame(records)
    #         return df
            
    def get_master_df(self):
        engine = get_db_engine()
        with engine.connect() as connection:
            mod_state_agent_query = text("SELECT * FROM master_module_state_agent;")
            correct_sop_query = text("SELECT * FROM sop_feedback;")

            mod_state_agent_records = connection.execute(mod_state_agent_query).fetchall()
            mod_state_agent_df = pd.DataFrame(mod_state_agent_records)
            
            correct_sop_records = connection.execute(correct_sop_query).fetchall()
            correct_sop_df = pd.DataFrame(correct_sop_records)
            
            selected_columns = ['module', 'state', 'agent', 'project', 'modified_generated_sop']
            master_df = pd.merge(correct_sop_df, mod_state_agent_df, left_on='msa_id', right_on='id', how='left')[selected_columns]
            return master_df

    # def decode_url(self, url):
    #     decoded_url = unquote(url)
    #     return decoded_url

    # # Function to return wiki content text based on title and identifier match
    # def get_match_records(self, df, title, identifier):
    #     filt_df = df[(df['title'] == title) & (df['identifier'] == identifier)]
    #     try:
    #         content = filt_df.iloc[0]['text']
    #         return content
    #     except:
    #         return 'Not able to find record for title and identifier match'
        
    # def get_wiki_content(self):
    #     df = self.get_master_df()
    #     df['decoded_correct_url'] = df['correct_sop'].apply(self.decode_url)
    #     df['title'] = df['decoded_correct_url'].str.split('/').str[-1]
    #     df['identifier'] = df['decoded_correct_url'].str.split('/').str[-3]
    #     # get df of identifer, title and text
    #     title_identifier_text_df = self.get_title_identifier_text_df()
    #     df['text'] = df.apply(lambda row: self.get_match_records(title_identifier_text_df, row['title'], row['identifier']), axis=1)
    #     return df

# obj = GetFeedbackDataCopy()
# obj.get_master_df()