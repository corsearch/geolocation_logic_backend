# -*- coding: utf-8 -*-

"""
General purpose analysis utilities
is_sublist: determine whether one list is a sublist of the other
get_sequences: construct contiguous sequences from a supplied list of indexes
load_json: load data from a specified JSON file
load_fixture: load fixture data from a JSON file and save it using the
specified saver
zipfile_obj: return a zipfile object when the zip folder is nested within
another zip folder
open_file: return the file pointer appropriately if the file is inside a
zipped folder (egg distribution)
"""
import dateutil
import hashlib
import io
import inspect
from itertools import chain, permutations
import json
import logging
import os


log = logging.getLogger(__name__)


def is_sublist(list1, list2):
    """
    Determine whether the first list is a subvlist of the second list
    :param list1: the first list
    :param list2: the second list
    :return: True if the first list is a subvlist of the second list,
    otherwise False
    """
    return sum([list1 == list2[i: i + len(list1)] for i in range(len(list2) -
                                                                 len(list1) +
                                                                 1)]) > 0


def get_sequences(index_list, unique_values=True):
    """
    Construct contiguous sequences from a supplied list of indexes.
    For example:
    [1, 2, 5, 6, 7, 9, 10, 11, 15] would give [[1, 2], [5, 6, 7], [9, 10, 11],
    [15]]
    :param index_list: the input list of indexes
    :param unique_values: remove any duplicate indexes in the input
    (default: True)
    :return: a list containing all contiguous sequences of indexes
    """
    if not index_list:
        return index_list
    sorted_indexes = sorted(set(index_list) if unique_values else index_list)
    sequences = [[sorted_indexes[0]]]
    for index in sorted_indexes[1:]:
        if index == sequences[-1][-1] + 1:
            sequences[-1].append(index)
        else:
            sequences.append([index])
    return sequences


def combine_words(words, permute=False):
    """
    Generate all possible combinations or permutations of the supplied words,
    including keeping all words separate, and
    combining some or all of the words into composite words.  For example:
    if permute is True:
    (fred, bill, joe) -> 24 permutations, from (joe, bill, fred) etc, through
    (billjoe, fred) etc, to (joefredbill) etc.
    if permute is False:
    (fred, bill, joe) -> 13 permutations: (bill, fred, joe), 6 combinations
    such as (billjoe, fred) etc, plus
    6 combinations such as (joefredbill) etc.
    :param words: the words to be combined
    :param permute: whether to allow permutations (default False)
    :return: the set of permutations of the words
    """
    indexes = [i for i in range(len(words))]
    groupings = chain.from_iterable(permutations(
        indexes, num) for num in range(len(indexes) + 1))
    text_set = set()
    for group in [g for g in groupings if g]:
        group_string = ''.join([words[i] for i in group])
        others = [words[i] for i in range(len(words)) if i not in group]
        all_elements = permutations([group_string] + others)
        for element in all_elements:
            if permute:
                text_set.add(element)
            else:
                sorted_element = tuple(sorted(element))
                text_set.add(sorted_element)
    return text_set


def load_json(descriptor, folder):
    """
    Load data from a specified JSON file
    :param descriptor: the descriptor for the JSON file (the file name minus
    the 'JSON' extension)
    :param folder: the folder containing the JSON file
    :return: the objects loaded from the JSON file
    """
    def decode_datetime(dct):
        if '__datetime__' in dct or '__date__' in dct:
            parsed = dateutil.parser.parse(dct['iso'])
            return parsed.date() if '__date__' in dct else parsed
        return dct

    with io.open(os.path.join(folder, '.'.join([descriptor, 'json'])),
                 mode='r',
                 encoding='utf-8') as json_file:
        try:
            return json.load(json_file, object_hook=decode_datetime)
        except ValueError as e:
            log.error("Could not load %s: %s" % (json_file, e))
            return None


def load_fixture(saver, fixture_file, fixture_folder, patch_file=None):
    """
    Load fixture data from a JSON file and save it using the specified saver
    (the save function for the model that is
    being populated by the fixture).
    :param saver: the model class or member function that is responsible for
    saving records,
                  i.e. <object>.<save_function>, where <object> may be a class
                  or a class instance
    :param fixture_file: the descriptor for the JSON fixture file (the file
    name minus the 'JSON' extension)
    :param fixture_folder: the folder containing the JSON fixture file
    :param patch_file: the descriptor for the JSON file that contains any
    patches that need to be made to
                       individual objects following the initial fixture load
    :return: None
    """
    # The fixture data defines the keys for each record explicitly; these are
    # extracted and returned so that the
    # fixture records can be queried within the tests.  The key items are then
    # 'demoted' so that they become normal
    # attributes of the record.
    # The save mechanism will vary depending upon whether the saver function
    # belongs to the class or to the class
    # instance - this is determined by inspection, and the record is passed to
    # the saver in the appropriate manner.
    # Some data cannot be successfully saved using this approach, probably
    # where the saver function has been defined
    # for use by scrapers, and additional checks/updates are made before the
    # data is committed.  To get round this
    # issue, an optional patch mechanism is used.  For each record in the
    # patch, the object is first queried from
    # the database, updated as needed and saved back.
    fixture = load_json(fixture_file, fixture_folder)
    keys = []
    for record in fixture:
        keys.append(record['keys'])
        record.update(record['keys'])
        del record['keys']
    if inspect.isclass(saver.__self__):
        for record in fixture:
            saver(**record)
    else:
        for record in fixture:
            saver.__self__.__dict__.update(record)
            saver()
    if patch_file:
        patches = load_json(patch_file, fixture_folder)
        model_class = saver.__self__ if inspect.isclass(
            saver.__self__) else saver.__self__.__class__
        for patch in patches:
            patchable = model_class.objects.get(**patch['keys'])
            for attr, value in patch['updates'].items():
                setattr(patchable, attr, value)
            patchable.save()
    return keys


# def shingles(string, length):
#     # move to nlp_utils
#     # TODO try to avoid only punctuations marks and retain non latin
#       characters
#     string_an = ''.join(c for c in string if c.isalnum())
#     return [string_an[i:i + length] for i in range(len(string_an) - length
#     + 1)]


def md5_hash(string):
    # Return the MD5 hash (32 hex characters) of the supplied string.
    return hashlib.md5(string.encode("utf-8")).hexdigest()


# def jaccard_similarity(string1, string2, shingle_length=3):
#     shingles1 = set(shingles(string1, length=shingle_length))
#     shingles2 = set(shingles(string2, length=shingle_length))
#     if not shingles1 or not shingles2:
#         return 0.
#     return len(shingles1 & shingles2) / len(shingles1 | shingles2)


def jaccard_similarity(string1, string2):

    # keep only alpha_numeric
    def shingles(string_value): return ''.join(
        filter(str.isalnum, string_value))

    shingles1 = set(shingles(string1))
    shingles2 = set(shingles(string2))

    if not shingles1 or not shingles2:
        return 0.
    return len(shingles1 & shingles2) / len(shingles1 | shingles2)


def metrics_required(slot):
    """
    Determine for given slot, if we need to log the slot-value in the
    kibanna metrics.
    :param slot: Slot which needs to be checked (if the slot-value pair need
    to be logged in kibana metrics)
    :return: Boolean
    """
    slots_for_metrics = ['telephone', 'email', 'address', 'latlong']
    return slot in slots_for_metrics


def cast_to_boolean(value):
    try:
        return value.lower() in ['true', 'yes', 'y', 't', '1']
    except AttributeError:
        # Allow boolean values to be returned as is
        return bool(value)


def is_integer(s):
    try:
        int(s)
        return True
    except (TypeError, ValueError):
        return False


def update_members(dict1, dict2):
    """
    Update the dicts in a dict of dicts
    :param dict1: the dict to be updated for each member
    :param dict2: the dict with additional values for each member
    :return: the updated dict1
    """
    for key in dict2:
        dict1[key].update(dict2[key])
    return dict1
