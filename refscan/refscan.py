import typer
from typing_extensions import Annotated
from dataclasses import dataclass, field
from collections import UserList

from rich.console import Console
from rich.table import Table
from pymongo import MongoClient, timeout
from linkml_runtime import SchemaView
from nmdc_schema.nmdc_data import get_nmdc_schema_definition

app = typer.Typer(
    help="Scan an NMDC MongoDB database for referential integrity issues.",
    add_completion=False,  # hides the shell completion options from `--help` output
    rich_markup_mode="markdown",  # enables use of Markdown in docstrings and CLI help
)

# Instantiate a Rich console for fancy console output.
# Reference: https://rich.readthedocs.io/en/stable/console.html
console = Console()


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


def get_collection_names(mongo_client: MongoClient, database_name: str, verbose: bool = True) -> list[str]:
    """
    Returns the names of the collections that exist in the specified database.
    """
    db = mongo_client.get_database(database_name)
    collection_names = db.list_collection_names()

    if verbose:
        console.print(f"MongoDB collections: {len(collection_names)}")

    return collection_names


def get_database_class_slot_names_from_schema(
        schema_view: SchemaView,
        distinct: bool = False,
        verbose: bool = True
) -> list[str]:
    """
    Returns the names of the slots of the `Database` class in the specified `SchemaView`.

    :param schema_view: A SchemaView instance
    :param distinct: Whether to filter out duplicate names
    :param verbose: Whether to show verbose output
    """
    database_el = schema_view.get_element("Database")  # TODO: Why is a string not an `ElementName`?
    slot_names = database_el.slots

    if distinct:
        slot_names = list(set(slot_names))  # filter out duplicate names

    if verbose:
        console.print(f"Schema database slots{' (distinct):' if distinct else ':'} {len(slot_names)}")

    return slot_names


def get_common_values(list_a: list, list_b: list) -> list:
    """
    Returns only the items that are present in _both_ lists.
    """
    return [a for a in list_a if a in list_b]


def get_names_of_classes_whose_instances_can_be_stored_in_slot(
        schema_view: SchemaView,
        slot_name: str,
        verbose: bool = False
) -> list[str]:
    """
    Returns the names of the classes whose instances can be stored in the slot having the specified name.
    """
    slot_definition = schema_view.get_slot(slot_name)
    slot_range = slot_definition.range

    # Get the name of the class and of each of its descendants, whose instances can populate this slot.
    class_names_valid_for_slot = schema_view.class_descendants(slot_range)  # includes own class name

    if verbose:
        console.print(f"{slot_name} ({len(class_names_valid_for_slot)}): {class_names_valid_for_slot=}")

    return class_names_valid_for_slot


@dataclass(frozen=True, order=True)
class Reference:
    """
    A generic reference to a document in a collection.

    Note: `frozen` means the instances are immutable.
    Note: `order` means the instances have methods that help with sorting. For example, an `__eq__` method that
          can be used to compare instances of the class as thought they were tuples of those instances' fields.
    """
    source_collection_name: str = field()
    source_field_name: str = field()
    target_collection_name: str = field()
    target_class_name: str = field(default="")


class ReferenceList(UserList):
    """
    A list of references.

    Note: `UserList` is a base class that facilitates the implementation of custom list classes.
          One thing it does is enable sorting via `sorted(the_list)`.
    """
    pass


@app.command("scan")
def scan(
        database_name: Annotated[str, typer.Option(
            help="Name of the database.",
        )] = "nmdc",
        mongo_uri: Annotated[str, typer.Option(
            envvar="MONGO_URI",
            help="Connection string for accessing the MongoDB server. If you have Docker installed, "
                 "you can spin up a temporary MongoDB server at the default URI by running: "
                 "`$ docker run --rm --detach -p 27017:27017 mongo`",
        )] = "mongodb://localhost:27017",
):
    # Connect to the MongoDB server and verify the database is accessible.
    mongo_client = connect_to_database(mongo_uri, database_name)

    # Identify the collections in the database.
    # e.g. ["study_set", "foo_set", ...]
    mongo_collection_names = get_collection_names(mongo_client, database_name)

    # Make a `SchemaView` that we can use to inspect the schema.
    schema_view = SchemaView(get_nmdc_schema_definition())

    # Identify the distinct slots of the `Database` class in the schema.
    # e.g. ["study_set", "bar_set", ...]
    schema_database_slot_names = get_database_class_slot_names_from_schema(schema_view, distinct=True)

    # Get the intersection of the two.
    # e.g. ["study_set", ...]
    collection_names: list[str] = get_common_values(mongo_collection_names, schema_database_slot_names)
    console.print(f"MongoDB collections described by schema: {len(collection_names)}")

    # Determine the names of the schema classes of which instances can be stored in each collection.
    collection_name_to_class_names = {}
    for collection_name in collection_names:
        class_names = get_names_of_classes_whose_instances_can_be_stored_in_slot(schema_view, slot_name=collection_name)
        collection_name_to_class_names[collection_name] = class_names

    # Determine the names of those schema classes' slots that can be foreign keys (i.e. references to class instances).
    references = ReferenceList()
    for collection_name, class_names in collection_name_to_class_names.items():
        for class_name in class_names:
            class_definition = schema_view.get_class(class_name)
            slot_names = class_definition.slots
            for slot_name in slot_names:
                # Get the slot definition in the context of its use on this particular class.
                slot_definition = schema_view.induced_slot(slot_name=slot_name, class_name=class_name)
                slot_range = slot_definition.range

                # If the slot's range is not a class name (e.g. it's an Enum instead), abort processing this slot.
                if slot_range not in schema_view.all_classes():
                    continue

                # Make a list consisting of the name of that class and the name of each of that class's descendants.
                # Example: "Animal" -> ["Animal", "Dog", "Cat", "Snake", "Husky", "Poodle"]
                # This is effectively a list of the names of the classes whose instances can be stored in this slot.
                class_names_valid_for_slot = schema_view.class_descendants(slot_range)  # includes own class name

                # For each class whose instances can be stored in any collection, record it as a reference.
                for class_name_valid_for_slot in class_names_valid_for_slot:
                    for referenced_collection_name, class_names_valid_for_referenced_collection in collection_name_to_class_names.items():
                        if class_name_valid_for_slot in class_names_valid_for_referenced_collection:
                            reference = Reference(collection_name,
                                                  slot_name,
                                                  referenced_collection_name,
                                                  class_name_valid_for_slot)
                            references.append(reference)

    # Display a table of references.
    table = Table(show_footer=True)
    table.add_column("Base collection", footer=f"{len(list(set(references)))} rows")
    table.add_column("Field")
    table.add_column("Referenced collection")
    table.add_column("Referenced class")
    for reference in sorted(list(set(references))):
        table.add_row(reference.source_collection_name,
                      reference.source_field_name,
                      reference.target_collection_name,
                      reference.target_class_name)
    console.print(table)

    # TODO: Perform the reference checks, using that list of references to streamline the process.

    # Close the connection to the MongoDB server.
    mongo_client.close()


if __name__ == "__main__":
    app()
