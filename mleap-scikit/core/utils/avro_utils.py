__author__ = 'mikhail.semeniuk'
import os.path
from abc import abstractmethod
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import datetime
import fnmatch
import ast
import errno
import pandas as pd


class AvroUtils(classmethod):
    """
    Interface/Method for classes that implement Avro functions
    """
    def __init__(self, schema_file, avro_file):
        """
        @type schema: ste
        @type avro_file: str
        schema: AVRO Schema of the object
        avro_file: location of the .avro file to write/read from
        """
        self.schema = schema_file
        self.avro_file = avro_file

    @abstractmethod
    def get_overlap_fields(self, data_object):
        schema = avro.schema.parse(open(self.schema).read())
        schema_fields = [x.name for x in schema.fields]
        overlap = list(set(schema_fields) & set(data_object.__dict__.keys()))
        return overlap

    @abstractmethod
    def populate_schema_fields(self, data_object):
        schema = avro.schema.parse(open(self.schema).read())
        schema_fields = [x.name for x in schema.fields]
        schema_nulls = [None for x in schema_fields]
        res = dict(zip(schema_fields, schema_nulls))
        if isinstance(data_object, dict):
            return data_object
        else:
            for key in data_object.__dict__.keys():
                res[key] = data_object.__dict__[key]
        return res

    @abstractmethod
    def write_df_to_avro(self, df):
        """
        Creates and writes an avro file
        @type data_objects: list
        """
        data_objects = df.T.to_dict()
        nrecs = 0
        schema = avro.schema.parse(open(self.schema).read())
        #check that the path that we want to write to actually exists
        path = '/'.join(self.avro_file.split('/')[:-1])
        if not os.path.exists(path):
            try:
                os.makedirs(path)
            except OSError as exc:
                if exc.errno == errno.EEXIST and os.path.isdir(path):
                    pass
                else:
                    raise
        #check if the file by he same name is already in that directory
        if os.path.isfile(self.avro_file):
            #Ammend name with a suffix of current minute + second
            now = datetime.datetime.now()
            min_sec_str = "%s%s" % (now.minute, now.second)
            self.avro_file = self.avro_file.replace('.avro', "_%s.avro" % (min_sec_str))


        writer = DataFileWriter(open(self.avro_file, "w"), DatumWriter(), schema)

        for obj in data_objects:
            avro_dict = self.populate_schema_fields(data_objects[obj])
            writer.append(avro_dict)
            nrecs += 1

        writer.close()
        print "%s records written to %s" % (nrecs, self.avro_file)

    @abstractmethod
    def write_dict_to_avro(self, data_objects):
        """
        Creates and writes an avro file
        @type data_objects: list
        """
        nrecs = 0
        schema = avro.schema.parse(open(self.schema).read())
        #check that the path that we want to write to actually exists
        path = '/'.join(self.avro_file.split('/')[:-1])
        if not os.path.exists(path):
            try:
                os.makedirs(path)
            except OSError as exc:
                if exc.errno == errno.EEXIST and os.path.isdir(path):
                    pass
                else:
                    raise
        #check if the file by he same name is already in that directory
        if os.path.isfile(self.avro_file):
            #Ammend name with a suffix of current minute + second
            now = datetime.datetime.now()
            min_sec_str = "%s%s" % (now.minute, now.second)
            self.avro_file = self.avro_file.replace('.avro', "_%s.avro" % (min_sec_str))


        writer = DataFileWriter(open(self.avro_file, "w"), DatumWriter(), schema)

        for obj in data_objects:
            avro_dict = self.populate_schema_fields(obj)
            writer.append(avro_dict)
            nrecs += 1

        writer.close()
        print "%s records written to %s" % (nrecs, self.avro_file)

    def _load_avro_file_to_dict(self, avro_file=None, min_ix=0):
        """
        Will grab the path of self.avro file to read the data into a
        dictionary
        """
        if avro_file is None:
            avro_file = self.avro_file
        reader = DataFileReader(open(avro_file, "r"), DatumReader())
        ix = min_ix
        res = dict()
        for rec in reader:
            res[ix] = rec
            ix += 1
        reader.close()
        return (ix, res)

    @abstractmethod
    def load_avro_file_to_dict(self, min_ix=0):
        return self._load_avro_file_to_dict(min_ix)[1]

    @abstractmethod
    def load_avro_path_to_dict(self, path):
        """
        Will itterate through the path + children and load all of the avro files
        that have a matching schema name
        """
        matches = []
        schema = avro.schema.parse(open(self.schema).read())
        #First get all of the files that match the .avro suffix
        for root, dirnames, filenames in os.walk(path):
            for filename in fnmatch.filter(filenames, '*.avro'):
                matches.append(os.path.join(root, filename))

        filtered_matches = []
        for f in matches:
            reader = DataFileReader(open(f, "r"), DatumReader())
            try:
                if ast.literal_eval(reader.meta['avro.schema'])['name'] == schema.name:
                    filtered_matches.append(f)
            except ValueError:
                try:
                    if schema.name in reader.meta['avro.schema'][:150]:
                        filtered_matches.append(f)
                except:
                    print "Failing to find a avro schema name match"
                    pass
            reader.close()

        #read the data from all of the avro files into the dictionary
        res_dict = dict()
        ix = 0
        for f in filtered_matches:
            res = self._load_avro_file_to_dict(f, ix)
            ix = res[0]
            res_dict = dict(res_dict.items() + res[1].items())
        return res_dict


    @abstractmethod
    def load_dict_to_pandas_df(self, res_dict):
        """
        @type res_dict: dict
        """
        df = pd.DataFrame.from_dict(res_dict, orient='index')
        return  df


    @abstractmethod
    def load_avro_path_files_to_df(self, root_path, unique_col=None):
        """
        Takes a "root" path and searches for all of the matching avro files, then loads
        them into a pandas dataframe. Optionally, if unique_col is passed through, then it will
        dedupe on those columns.
        @type root_path: str
        @type unique_col: list, str
        >>>places_avro_utils = FBAvroData("./fb_places.avsc", outfile_places)
        >>>df_places = places_avro_utils.load_avro_path_files_to_df("./picndata/places/raw/facebook/search/")
        """
        res_dict = self.load_avro_path_to_dict(root_path)
        df = self.load_dict_to_pandas_df(res_dict)

        if unique_col is not None:
            df.drop_duplicates(cols=unique_col, take_last=True, inplace=True)

        return df