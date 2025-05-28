from pg8000 import literal

def format_dict

def get_updated_data(db, table, last_extract):
    tab_data = db.run(f"""SELECT * FROM {literal(table)}
                   WHERE last_updated > {literal(last_extract)};""")
    keys = [column["name"] for column in db.columns]
    new_dict = {key:tab_data[key] for key in keys}

    return new_dict