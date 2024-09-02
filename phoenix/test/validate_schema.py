import pandas as pd


class SchemaValidator:
    """
    A class to validate the schema of extracted data against a predefined database schema.

    The `SchemaValidator` class checks if the fields and data types of the extracted data conform to a specified 
    database schema. It uses a source-based data type mapping to interpret field types and validate data accordingly.

    Attributes:
        logger: A logging instance to log warnings and errors.
        _schema (dict): A dictionary that maps field names to their expected data types.
        _data_type_mapping (dict): A mapping of source-specific data types to Python data types.
        _database_schema (dict): A dictionary representing the expected schema of the database.
        _extracted_fields (list): A list of field names extracted from the source.

    Methods:
        map_schema(): Maps extracted fields to their expected data types based on the database schema and source.
        validate_schema(extracted_data: pd.DataFrame): Validates the extracted data against the expected schema.
    """

    def __init__(self, source: str, database_schema: dict, extracted_fields: list, logger):
        """
        Initializes the SchemaValidator with the given parameters.

        Args:
            source (str): The data source type, used to determine data type mappings (e.g., 'elasticsearch').
            database_schema (dict): The schema dictionary representing field names and their expected data types.
            extracted_fields (list): A list of field names that have been extracted from the data source.
            logger: A logger instance to capture warnings and information.
        """
        self.logger = logger
        self._schema = {}

        self._data_type_mapping = {}
        self.data_type_mapping = source

        self._database_schema = {}
        self.database_schema = database_schema

        self._extracted_fields = {}
        self.extracted_fields = extracted_fields

        self.map_schema()

    @property
    def schema(self):
        """dict: Returns the schema that maps field names to their expected data types."""
        return self._schema
    
    def map_schema(self):
        """Maps extracted fields to their expected data types based on the database schema and source type.

        This method iterates over the extracted fields and maps them to their expected data types using the database schema
        and data type mapping. It populates the `_schema` attribute with field names and their corresponding data types.
        """
        for field in self.extracted_fields:
            data_type = self.database_schema.get(field)
            expected_data_type = self.data_type_mapping.get(data_type)
            if expected_data_type is None:
                continue

            self._schema[field] = expected_data_type

    @property
    def database_schema(self):
        """dict: Returns the database schema with field names and expected data types."""
        return self._database_schema

    @database_schema.setter
    def database_schema(self, schema: dict):
        """Sets the database schema.

        Args:
            schema (dict): A dictionary representing the database schema.
        """
        self._database_schema = schema

    @property
    def data_type_mapping(self):
        """dict: Returns the mapping of source-specific data types to Python data types."""
        return self._data_type_mapping

    @data_type_mapping.setter
    def data_type_mapping(self, source: str):
        """Sets the data type mapping based on the data source.

        Args:
            source (str): The data source type (e.g., 'elasticsearch') to determine the data type mapping.
        """
        data_type_maps = {
            'elasticsearch': {
                'boolean': bool,
                'date': str,
                'float': float,
                'long': int,
                'text': str
            }
        }
        self._data_type_mapping = data_type_maps.get(source)

    @property
    def extracted_fields(self):
        """dict: Returns the extracted fields with their expected data types."""
        return self._extracted_fields

    @extracted_fields.setter
    def extracted_fields(self, fields):
        """Sets the extracted fields.

        Args:
            fields (list): A list of field names extracted from the data source.
        """
        self._extracted_fields = fields

    def validate_schema(self, extracted_data: pd.DataFrame):
        """
        Validates the extracted data against the expected schema.

        This method checks that each field in the extracted data matches the expected data type defined in the schema.
        It logs warnings if:
        - A field expected in the schema is missing from the extracted data.
        - Any row in a field has a data type that does not match the expected type.

        Args:
            extracted_data (pd.DataFrame): The DataFrame containing the extracted data to be validated.

        Returns:
            None: Logs warnings for any discrepancies found.
        """
        def convert_data_type(value):
            """
            Converts a value to a specified data type based on its original data type.

            This function checks the data type of the input value and converts it to a predefined Python data type 
            if a mapping exists. The function currently supports conversion of 'float64' and 'int64' types to 
            Python's built-in `float` and `int` types, respectively. If the value's data type does not match any 
            predefined mapping, the original value is returned without modification.

            Args:
                value: The input value to be checked and possibly converted. This value can be of any data type.

            Returns:
                The value converted to the mapped data type if a mapping is found; otherwise, returns the original value.
            """
            data_types = {
                'float64': float,
                'int64': int
            }
            data_type_name = type(value).__name__
            data_type = data_types.get(data_type_name)
            if not data_type:
                return value

            return data_type(value)

        for field_name, expected_data_type in self.schema.items():
            if field_name not in extracted_data:
                self.logger.warning(f"Warning: Field '{field_name}' is missing from the extracted data.")
                continue

            column_data = extracted_data[field_name]
            type_mismatch = column_data.apply(lambda x: not isinstance(convert_data_type(x), expected_data_type))

            if type_mismatch.any():
                mismatches = type_mismatch[type_mismatch].index.to_list()
                self.logger.warning(
                    f"Warning: Field '{field_name}' contains {len(mismatches)} rows with data type mismatches. "
                    f"Expected type: '{expected_data_type}', mismatched row indices: {mismatches}."
                )
