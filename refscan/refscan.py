import typer
from typing_extensions import Annotated
from dataclasses import dataclass, field
from collections import UserList
from itertools import groupby

from rich.console import Console
from rich.table import Table
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from pymongo import MongoClient, timeout
from pymongo.database import Database
from linkml_runtime import SchemaView
from nmdc_schema.nmdc_data import get_nmdc_schema_definition

app = typer.Typer(
    help="Scan a LinkML schema-compliant MongoDB database for referential integrity issues.",
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

    :param schema_view: A `SchemaView` instance
    :param distinct: Whether to filter out duplicate names
    :param verbose: Whether to show verbose output
    """
    class_definition = schema_view.get_class("Database")
    slot_names = class_definition.slots

    if distinct:
        slot_names = list(set(slot_names))  # filter out duplicate names

    if verbose:
        console.print(f"Schema database slots{' (distinct):' if distinct else ':'} {len(slot_names)}")

    return slot_names


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
class Violation:
    """
    A specific reference that lacks integrity.
    """
    source_collection_name: str = field()
    source_field_name: str = field()
    source_document_object_id: str = field()
    source_document_id: str = field()
    target_id: str = field()


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

    def get_source_collection_names(self) -> list[str]:
        """
        Returns the distinct `source_collection_names` values among all references in the list.
        """
        distinct_source_collection_names = []
        for reference in self.data:
            if reference.source_collection_name not in distinct_source_collection_names:
                distinct_source_collection_names.append(reference.source_collection_name)
        return distinct_source_collection_names

    def get_source_field_names_of_source_collection(self, collection_name: str) -> list[str]:
        """
        Returns the distinct source field names of the specified source collection.
        """
        distinct_source_field_names = []
        for reference in self.data:
            if reference.source_collection_name == collection_name:
                if reference.source_field_name not in distinct_source_field_names:
                    distinct_source_field_names.append(reference.source_field_name)
        return distinct_source_field_names

    def get_target_collection_names(self, source_collection_name: str, source_field_name: str) -> list[str]:
        """
        Returns the distinct target collection names of the specified source collection/source field combination.
        """
        distinct_target_collection_names = []
        for reference in self.data:
            if reference.source_collection_name == source_collection_name and \
                    reference.source_field_name == source_field_name:
                if reference.target_collection_name not in distinct_target_collection_names:
                    distinct_target_collection_names.append(reference.target_collection_name)
        return distinct_target_collection_names

    def get_groups(self, field_names: list[str]):
        """
        Returns an iterable of groups, where each group has a distinct combination of values in the specified fields.

        Note: This method can be used to "consolidate" references that have the same source collection name,
              source field name, and target collection name (i.e. ones that only differ by target class name).
        """

        def make_group_key(reference: Reference) -> tuple:
            """Helper function that returns a key that can be used to group references."""
            values = []
            for field_name in field_names:
                if not hasattr(reference, field_name):
                    raise ValueError(f"No such field: {field_name}")
                values.append(getattr(reference, field_name))
            return tuple(values)

        groups = groupby(sorted(self.data), key=make_group_key)
        return groups


def check_whether_document_having_id_exists_among_collections(
        db: Database,
        collection_names: list[str],
        document_id: str
) -> bool:
    """
    Checks whether any documents having the specified `id` (in its `id` field) exists
    in any of the specified collections.
    """
    exists = False
    for collection_name in collection_names:
        if db.get_collection(collection_name).count_documents({'id': document_id}) > 0:
            exists = True
            break
    return exists


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
        verbose: Annotated[bool, typer.Option(
            help="Show verbose output.",
        )] = False,
):
    """
    Scans a LinkML schema-compliant MongoDB database for referential integrity issues.
    """

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
    groups = references.get_groups(["source_collection_name", "source_field_name", "target_collection_name"])
    rows: list[tuple[str, str, str, str]] = []
    for key, group in groups:
        target_class_names = [ref.target_class_name for ref in group]
        row = (key[0], key[1], key[2], ", ".join(target_class_names))
        rows.append(row)
    table = Table(show_footer=True)
    table.add_column("Source collection", footer=f"{len(rows)} rows")
    table.add_column("Source field")
    table.add_column("Target collection")
    table.add_column("Target class(es)")
    for row in rows:
        table.add_row(*row)
    if verbose:
        console.print(table)

    # Define a progress bar that includes the elapsed time and M-of-N completed count.
    custom_progress = Progress(
        TextColumn("[progress.description]{task.description}"),
        MofNCompleteColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        BarColumn(),
        TimeElapsedColumn(),
        TextColumn("elapsed"),
        TimeRemainingColumn(elapsed_when_finished=True),
        TextColumn("remaining"),
        console=console,
        refresh_per_second=1,
    )

    # Process each collection.
    db = mongo_client.get_database(database_name)
    violations = []
    with custom_progress as progress:
        for source_collection_name in references.get_source_collection_names():
            collection = db.get_collection(source_collection_name)

            # Process each document that has any of the field that can contain a reference.
            source_field_names = references.get_source_field_names_of_source_collection(source_collection_name)
            or_terms = [{field_name: {'$exists': True}} for field_name in source_field_names]
            query_filter = {'$or': or_terms}
            if verbose:
                console.print(f"{query_filter=}")

            # Set up the progress bar for this task.
            num_relevant_documents = collection.count_documents(query_filter)
            task_id = progress.add_task(f"{source_collection_name}", total=num_relevant_documents)

            # Advance the progress bar by 0 (this makes it so that, even if there are 0 relevant documents,
            # that progress bar does not continue counting its "elapsed time" upward).
            progress.advance(task_id, advance=0)

            for document in collection.find(query_filter):

                # Advance the progress bar for the current task.
                progress.advance(task_id)

                source_document_object_id = document["_id"]
                source_document_id = document["id"] if "id" in document else None
                for field_name in source_field_names:
                    if field_name in document:
                        target_collection_names = references.get_target_collection_names(source_collection_name,
                                                                                         field_name)
                        # console.print(f"{source_collection_name}.{field_name} -> {target_collection_names}")

                        # Handle both the multiple-value and the single-value case.
                        if type(document[field_name]) is list:
                            target_ids = document[field_name]
                            for target_id in target_ids:
                                target_exists = check_whether_document_having_id_exists_among_collections(
                                    db,
                                    target_collection_names,
                                    target_id
                                )
                                if not target_exists:
                                    violation = Violation(source_collection_name=source_collection_name,
                                                          source_field_name=field_name,
                                                          source_document_object_id=source_document_object_id,
                                                          source_document_id=source_document_id,
                                                          target_id=target_id)
                                    console.print(f"Failed to find document having `id` '{target_id}' "
                                                  f"among collections: {target_collection_names}. "
                                                  f"{violation=}")
                                    violations.append(violation)
                        else:
                            target_id = document[field_name]
                            target_exists = check_whether_document_having_id_exists_among_collections(
                                db,
                                target_collection_names,
                                target_id
                            )
                            if not target_exists:
                                violation = Violation(source_collection_name=source_collection_name,
                                                      source_field_name=field_name,
                                                      source_document_object_id=source_document_object_id,
                                                      source_document_id=source_document_id,
                                                      target_id=target_id)
                                console.print(f"Failed to find document having `id` '{target_id}' "
                                              f"among collections: {target_collection_names}. "
                                              f"{violation=}")
                                violations.append(violation)

    # Print all the violations.
    console.print(violations)

    # Close the connection to the MongoDB server.
    mongo_client.close()


if __name__ == "__main__":
    app()
