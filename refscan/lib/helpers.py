from pymongo import MongoClient, timeout
from pymongo.database import Database
from linkml_runtime import SchemaView

from refscan.lib.constants import DATABASE_CLASS_NAME, console


def connect_to_database(mongo_uri: str, database_name: str, verbose: bool = True) -> MongoClient:
    """
    Returns a Mongo client. Raises an exception if the database is not accessible.
    """
    mongo_client: MongoClient = MongoClient(host=mongo_uri, directConnection=True)

    with (timeout(5)):  # if any message exchange takes > 5 seconds, this will raise an exception
        (host, port_number) = mongo_client.address

        if verbose:
            console.print(f'Connected to MongoDB server: "{host}:{port_number}"')

        # Check whether the database exists on the MongoDB server.
        if database_name not in mongo_client.list_database_names():
            raise ValueError(f'Database "{database_name}" not found on the MongoDB server.')

    return mongo_client


def get_collection_names_from_database(
        mongo_client: MongoClient,
        database_name: str,
        verbose: bool = True
) -> list[str]:
    """
    Returns the names of the collections that exist in the specified database.
    """
    db = mongo_client.get_database(database_name)
    collection_names = db.list_collection_names()

    if verbose:
        console.print(f"Existing collections: {len(collection_names)}")

    return collection_names


def get_collection_names_from_schema(
        schema_view: SchemaView,
        verbose: bool = True
) -> list[str]:
    """
    Returns the names of the slots of the `Database` class that correspond to database collections.

    :param schema_view: A `SchemaView` instance
    :param verbose: Whether to show verbose output
    """
    collection_names = []

    for slot_name in schema_view.class_slots(DATABASE_CLASS_NAME):
        slot_definition = schema_view.induced_slot(slot_name, DATABASE_CLASS_NAME)

        # Filter out any hypothetical (future) slots that don't correspond to a collection (e.g. `db_version`).
        if slot_definition.multivalued and slot_definition.inlined_as_list:
            collection_names.append(slot_name)

        # Filter out duplicate names. This is to work around the following issues in the schema:
        # - https://github.com/microbiomedata/nmdc-schema/issues/1954
        # - https://github.com/microbiomedata/nmdc-schema/issues/1955
        collection_names = list(set(collection_names))

    if verbose:
        console.print(f"Collections described by schema: {len(collection_names)}")

    return collection_names


def get_common_values(list_a: list, list_b: list) -> list:
    """
    Returns only the items that are present in _both_ lists.

    >>> get_common_values([1, 2, 3], [4, 5])  # zero
    []
    >>> get_common_values([1, 2, 3], [3, 4, 5])  # one
    [3]
    >>> get_common_values([1, 2, 3, 4], [3, 4])  # multiple
    [3, 4]
    >>> get_common_values([1, 2, 3], [1, 2, 3])  # all
    [1, 2, 3]
    """
    return [a for a in list_a if a in list_b]


def check_whether_document_having_id_exists_among_collections(
        db: Database,
        collection_names: list[str],
        document_id: str
) -> bool:
    """
    Checks whether any documents having the specified `id` value (in its `id` field) exists
    in any of the specified collections.

    References:
    - https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html#pymongo.collection.Collection.find_one
    """
    document_exists = False
    query_filter = {"id": document_id}
    for collection_name in collection_names:
        document_exists = db.get_collection(collection_name).find_one(query_filter, projection=["_id"]) is not None
        if document_exists:  # if we found the document in this collection, there is no need to keep searching
            break
    return document_exists