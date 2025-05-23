# rewritten version of Максим Петренко
import re

# Character sets for Cyrillic and Latin alphabets
cyrillic_chars = set('ІіАаВЕеКМНОоРрСсТуХх')
latin_chars = set('IiAaBEeKMHOoPpCcTyXx')
full_cyrillic = set('АаБбВвГгДдЕеЄєЖжЗзІіЫыИиФфЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЭэЮюЯяЇїЬь')
full_latin = set('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz')
roman_numerals = set('CDMXLVI')

# Special characters specific to Cyrillic or Latin
special_cyrillic = full_cyrillic.difference(cyrillic_chars)
special_latin = full_latin.difference(latin_chars)

# Regular expression to match words
word_pattern = re.compile(r"(\w[\w']*\w|\w)")

def is_valid_alphabet_mix(word):
    """
    Check if the word contains characters from both Cyrillic and Latin alphabets.
    """
    unique_chars = set(word)
    return not (unique_chars.intersection(full_cyrillic) and unique_chars.intersection(full_latin))

def highlight_mismatched_chars(word):
    """
    Highlight mismatched characters by wrapping Latin characters in <f> tags
    and Cyrillic characters in <u> tags.
    """
    formatted_word = ''
    for char in word:
        if char in latin_chars:
            formatted_word += '<f>' + char + '</f>'
        if char in cyrillic_chars:
            formatted_word += '<u>' + char + '</u>'
    return formatted_word

def highlight_mismatched_in_context(word):
    """
    Highlight mismatched characters in the context of their alphabets.
    """
    formatted_word = ''
    for char in word:
        if char in latin_chars:
            formatted_word += '<mf>' + char + '</mf>'
        if char in cyrillic_chars:
            formatted_word += '<mu>' + char + '</mu>'
    return formatted_word

def convert_to_latin(word):
    """
    Convert Cyrillic characters in the word to Latin characters.
    """
    cyrillic_set = 'ІіАаВЕеКМНОоРрСсТуХх'
    latin_set = 'IiAaBEeKMHOoPpCcTyXx'
    translation_table = word.maketrans(cyrillic_set, latin_set)
    return word.translate(translation_table)

def convert_to_cyrillic(word):
    """
    Convert Latin characters in the word to Cyrillic characters.
    """
    latin_set = 'IiAaBEeKMHOoPpCcTyXx'
    cyrillic_set = 'ІіАаВЕеКМНОоРрСсТуХх'
    translation_table = word.maketrans(latin_set, cyrillic_set)
    return word.translate(translation_table)

def fix_homoglyph_errors(text):
    """
    Check the text for homoglyph errors (mixed Cyrillic and Latin characters)
    and fix them by converting between alphabets where necessary.
    """
    words = word_pattern.findall(text)
    for word in words:
        if not is_valid_alphabet_mix(word):
            word_chars_set = set(word)
            
            # Case 1: Potential Roman numeral
            if word_chars_set.issubset(roman_numerals) and bool(re.fullmatch(r'(\W|\b)((?:M{0,4})(?:CM|CD|D?C{0,3})(?:XC|XL|L?X{0,3})(?:IX|IV|V?I{0,3}))+(\b)', convert_to_latin(word))):
                print("Roman numeral detected, converting to Latin.")
                text = text.replace(word, convert_to_latin(word), 1)
            
            # Case 2: Cyrillic mixed with Latin characters
            elif word_chars_set.intersection(special_cyrillic) and word_chars_set.intersection(latin_chars) and not word_chars_set.intersection(special_latin):
                print("Cyrillic to Latin error detected, converting to Cyrillic.")
                text = text.replace(word, convert_to_cyrillic(word), 1)
            
            # Case 3: Latin mixed with Cyrillic characters
            elif word_chars_set.intersection(special_latin) and word_chars_set.intersection(cyrillic_chars) and not word_chars_set.intersection(special_cyrillic):
                print("Latin to Cyrillic error detected, converting to Latin.")
                text = text.replace(word, convert_to_latin(word), 1)
                
                # Format word based on predominant alphabet
                max_latin_count = len([char for char in word if char in latin_chars])
                max_cyrillic_count = len([char for char in word if char in cyrillic_chars])
                if max_latin_count > max_cyrillic_count:
                    print('More Latin characters, applying error formatting.')
                    text = text.replace(word, highlight_mismatched_chars(word), 1)

    return text