# source data location
source = 'dbfs:/FileStore/NN/data'

# raw target table write location
targettable_raw = 'prod_cz_re_raw'

# trusted target table write location
targettable_trusted = 'prod_cz_re_trusted'

# consumption view write location
targetview_consumption = 'vw_cz_re_consumption'

#czech letters mapping
czech_mapping = {
    'A': 'a', 'a': 'u',
    'Á': 'a', 'á': 'a',
    'B': 'b', 'b': 'b',
    'C': 'ts', 'c': 'ts',
    'Č': 'ch', 'č': 'ch',
    'D': 'd', 'd': 'd',
    'Ď': 'dy', 'ď': 'dy',
    'E': 'e', 'e': 'e',
    'É': 'ai', 'é': 'ai',
    'Ě': 'ye', 'ě': 'ye',
    'F': 'f', 'f': 'f',
    'G': 'g', 'g': 'g',
    'H': 'h', 'h': 'h',
    'CH': 'ch', 'ch': 'ch',
    'I': 'i', 'i': 'i',
    'Í': 'ee', 'í': 'ee',
    'J': 'y', 'j': 'y',
    'K': 'k', 'k': 'k',
    'L': 'l', 'l': 'l',
    'M': 'm', 'm': 'm',
    'N': 'n', 'n': 'n',
    'Ň': 'ny', 'ň': 'ny',
    'O': 'o', 'o': 'o',
    'Ó': 'oo', 'ó': 'oo',
    'P': 'p', 'p': 'p',
    'R': 'r', 'r': 'r',
    'Ř': 'rz', 'ř': 'rz',
    'S': 's', 's': 's',
    'Š': 'sh', 'š': 'sh',
    'T': 't', 't': 't',
    'Ť': 'ty', 'ť': 'ty',
    'U': 'u', 'u': 'u',
    'Ú': 'oo', 'ú': 'oo',
    'Ů': 'oo', 'ů': 'oo',
    'V': 'v', 'v': 'v',
    'Y': 'i', 'y': 'i',
    'Ý': 'ee', 'ý': 'ee',
    'Z': 'z', 'z': 'z',
    'Ž': 'zh', 'ž': 'zh',
    '_': '_'
}

# tuple of fields where a hierarchy is applied
colhierarchy = ('filetimestamp'
               ,'source_id'
               ,'source_path'
               ,'source_rundate'
               ,'norm_price'
               ,'location_text')
