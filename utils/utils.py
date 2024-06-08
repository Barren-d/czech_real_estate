import datetime
import os

class content:
    """
    content class containing source path, raw, trusted and consumption content
    """
    def __init__(self, path, raw, trusted):
        self.path = path
        self.raw = raw
        self.trusted = trusted

def add_metadata_to_dict(data, path):
    """
    Adds metadata to dict
    Args:
        data (dict): containing data
        path (str): source path
    """
    # add metadata to dict    
    data['filetimestamp'] = datetime.date.today()
    data['source_id'] = os.path.basename(path).replace('.html','')
    data['source_path'] = path
    data['source_rundate'] = datetime.datetime.today()
    # return the metadata
    return data

def extract_from_url(url):
    """
    collects response from url and returns response text
    Args:
        url (str): url of the source content
    Returns:
        soup (bs4.BeautifulSoup): object containing the html content
    """
    # get the response from path
    res = requests.get(path)
    # if response is ok
    if res.ok:
        # get the html from response
        log.info(f"sourced {path=} from remote path")
        return res.text
    # handle bad response
    else:
        raise ValueError(f"Bad response {res.status_code=}")

def extract_from_filestore(path):
    """
    copies file contents from Filestore to Driver Node and returns content text
    Args:
        path (str): dbfs storage path
    Returns:
        content (str): contents of the file
    """
    # extract base name from path
    filename = os.path.basename(path)
    # copy file from dbfs to driver node
    dbutils.fs.cp(path,
              f"file:///tmp/{filename}")
    # read content from driver node
    with open(f"/tmp/{filename}", 'r', encoding='utf-8') as f:
        content = f.read()
    # return content
    return content

def sort_by_hierarchy_first(cols, hierarchy):
    """
    sorts list elements first if they appear in the hierarchy,
    then alphabetically
    Args:
        cols (list<str>)
    Returns:
        list
    """
    # ranked fields sorted by the hierarchy tuple
    ranked = [col for col in cols if col in hierarchy]
    ranked = sorted(ranked, key=lambda x: hierarchy.index(x))
    # rest of fields sorted normally
    rest = [col for col in cols if col not in hierarchy]
    rest = sorted(rest)
    return ranked + rest
