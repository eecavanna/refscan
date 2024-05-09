from pathlib import Path
from typing import List, Optional
from typing_extensions import Annotated
from dataclasses import dataclass, field, fields, astuple
from collections import UserList
from itertools import groupby
import csv

import typer
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
    help="Scan the NMDC MongoDB database for referential integrity violations.",
    add_completion=False,  # hides the shell completion options from `--help` output
    rich_markup_mode="markdown",  # enables use of Markdown in docstrings and CLI help
)

# Instantiate a Rich console for fancy console output.
# Reference: https://rich.readthedocs.io/en/stable/console.html
console = Console()

# Note: This is the only schema class name hard-coded into this script.
DATABASE_CLASS_NAME = "Database"


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


class ViolationList(UserList):
    """
    A list of violations.
    """

    def dump_to_tsv_file(self, file_path: str | Path) -> None:
        """
        Helper function that dumps the violations to a TSV file at the specified path.
        """
        column_names = [field_.name for field_ in fields(Violation)]
        with open(file_path, "w", newline="") as tsv_file:
            writer = csv.writer(tsv_file, delimiter="\t")
            writer.writerow(column_names)  # header row
            for violation in self.data:
                writer.writerow(astuple(violation))  # data row


@dataclass(frozen=True, order=True)
class Reference:
    """
    A generic reference to a document in a collection.

    Note: `frozen` means the instances are immutable.
    Note: `order` means the instances have methods that help with sorting. For example, an `__eq__` method that
          can be used to compare instances of the class as thought they were tuples of those instances' fields.
    """
    source_collection_name: str = field()  # e.g. "study_set"
    source_class_name: str = field()  # e.g. "study_set"
    source_field_name: str = field()  # e.g. "part_of"
    target_collection_name: str = field()  # e.g. "study_set"
    target_class_name: str = field()  # e.g. "Study"


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

    def get_groups(self, field_names: list[str]) -> list[tuple[str, str, str, str, list[str]]]:
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

    def dump_to_tsv_file(self, file_path: str | Path) -> None:
        """
        Helper function that dumps the references to a TSV file at the specified path.
        """
        column_names = [field_.name for field_ in fields(Reference)]
        with open(file_path, "w", newline="") as tsv_file:
            writer = csv.writer(tsv_file, delimiter="\t")
            writer.writerow(column_names)  # header row
            for reference in self.data:
                writer.writerow(astuple(reference))  # data row


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
        # Reference: https://typer.tiangolo.com/tutorial/multiple-values/multiple-options/
        skip_source_collection: Annotated[Optional[List[str]], typer.Option(
            "--skip-source-collection", "--skip",
            help="Name of collection you do not want to search for referring documents. "
                 "Option can be used multiple times.",
        )] = None,
        # Reference: https://typer.tiangolo.com/tutorial/parameter-types/path/
        reference_report_file_path: Annotated[Optional[Path], typer.Option(
            "--reference-report",
            dir_okay=False,
            writable=True,
            readable=False,
            resolve_path=True,
            help="Filesystem path at which you want the program to generate its reference report.",
        )] = "references.tsv",
        violation_report_file_path: Annotated[Optional[Path], typer.Option(
            "--violation-report",
            dir_okay=False,
            writable=True,
            readable=False,
            resolve_path=True,
            help="Filesystem path at which you want the program to generate its violation report.",
        )] = "violations.tsv",
):
    """
    Scans the NMDC MongoDB database for referential integrity violations.
    """

    # Make a more self-documenting alias for the CLI option that can be specified multiple times.
    names_of_source_collections_to_skip: list[str] = [] if skip_source_collection is None else skip_source_collection

    # Connect to the MongoDB server and verify the database is accessible.
    mongo_client = connect_to_database(mongo_uri, database_name)

    # Identify the collections in the database.
    # e.g. ["study_set", "foo_set", ...]
    mongo_collection_names = get_collection_names(mongo_client, database_name)

    # Make a `SchemaView` that we can use to inspect the schema.
    schema_view = SchemaView(get_nmdc_schema_definition())

    # Get a list of collection names (technically, `Database` slot names) from the schema.
    # e.g. ["study_set", "bar_set", ...]
    schema_database_slot_names = get_collection_names_from_schema(schema_view)

    # Get the intersection of the two.
    # e.g. ["study_set", ...]
    collection_names: list[str] = get_common_values(mongo_collection_names, schema_database_slot_names)
    console.print(f"Existing collections described by schema: {len(collection_names)}")

    # For each collection, determine the names of the classes whose instances can be stored in that collection.
    collection_name_to_class_names = {}  # example: { "study_set": ["Study"] }
    for collection_name in collection_names:
        slot_definition = schema_view.induced_slot(collection_name, DATABASE_CLASS_NAME)
        name_of_eligible_class = slot_definition.range
        names_of_eligible_classes = schema_view.class_descendants(name_of_eligible_class)  # includes own class name
        collection_name_to_class_names[collection_name] = names_of_eligible_classes

    # Initialize the list of references. A reference is effectively a "foreign key" (i.e. a pointer).
    references = ReferenceList()

    # For each class whose instances can be stored in each collection, determine which of its slots can be a reference.
    sorted_collection_names_to_class_names = sorted(collection_name_to_class_names.items(),
                                                    key=lambda kv: kv[0])  # sort by key
    for collection_name, class_names in sorted_collection_names_to_class_names:
        for class_name in class_names:
            for slot_name in schema_view.class_slots(class_name):
                # Get the slot definition in the context of its use on this particular class.
                slot_definition = schema_view.induced_slot(slot_name=slot_name, class_name=class_name)

                # Determine the slot's "effective" range, by taking into account its `any_of` constraints (if defined).
                #
                # Note: The `any_of` constraints constrain the slot's "effective" range beyond that described by the
                #       induced slot definition's `range` attribute. `SchemaView` does not seem to provide the result
                #       of applying those additional constraints, so we do it manually here (if any are defined).
                #
                # Reference: https://github.com/orgs/linkml/discussions/2101#discussion-6625646
                #
                names_of_eligible_target_classes: list[str] = []
                if "any_of" in slot_definition and len(slot_definition.any_of) > 0:  # use the `any_of` attribute
                    for slot_expression in slot_definition.any_of:
                        if slot_expression.range in schema_view.all_classes():
                            own_and_descendant_class_names = schema_view.class_descendants(slot_expression.range)
                            names_of_eligible_target_classes.extend(own_and_descendant_class_names)
                else:  # use the `range` attribute
                    if slot_definition.range not in schema_view.all_classes():  # if it's not a class name, abort
                        continue
                    else:
                        # Get the specified class name and the names of all classes that inherit from it.
                        own_and_descendant_class_names = schema_view.class_descendants(slot_definition.range)
                        names_of_eligible_target_classes.extend(own_and_descendant_class_names)

                # Remove duplicate class names.
                names_of_eligible_target_classes = list(set(names_of_eligible_target_classes))

                # For each of those classes whose instances can be stored in any collection, catalog a reference.
                for name_of_eligible_target_class in names_of_eligible_target_classes:
                    for target_collection_name, class_names_in_collection in collection_name_to_class_names.items():
                        if name_of_eligible_target_class in class_names_in_collection:
                            reference = Reference(source_collection_name=collection_name,
                                                  source_class_name=class_name,
                                                  source_field_name=slot_name,
                                                  target_collection_name=target_collection_name,
                                                  target_class_name=name_of_eligible_target_class)
                            references.append(reference)

    console.print(f"Slot-to-ID references described by schema: {len(references)}")

    # Create a reference report in TSV format.
    console.print(f"Writing reference report: {reference_report_file_path}")
    references.dump_to_tsv_file(file_path=reference_report_file_path)

    # Display a table of references.
    groups = references.get_groups(["source_collection_name",
                                    "source_class_name",
                                    "source_field_name",
                                    "target_collection_name"])
    rows: list[tuple[str, str, str, str, str]] = []
    for key, group in groups:
        target_class_names = list(set([ref.target_class_name for ref in group]))  # omit duplicate class names
        row = (key[0], key[1], key[2], key[3], ", ".join(target_class_names))
        rows.append(row)
    table = Table(show_footer=True)
    table.add_column("Source collection", footer=f"{len(rows)} rows")
    table.add_column("Source class")
    table.add_column("Source field")
    table.add_column("Target collection")
    table.add_column("Target class(es)")
    for row in rows:
        table.add_row(*row)
    if verbose:
        console.print(table)

    # Define a progress bar that includes the elapsed time and M-of-N completed count.
    # Reference: https://rich.readthedocs.io/en/stable/progress.html?highlight=progress#columns
    custom_progress = Progress(
        TextColumn("[progress.description]{task.description}"),
        TextColumn("[red]{task.fields[num_violations]}[/red] violations"),
        MofNCompleteColumn(),
        TextColumn("documents scanned"),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        BarColumn(),
        TimeElapsedColumn(),
        TextColumn("elapsed"),
        TimeRemainingColumn(elapsed_when_finished=True),
        TextColumn("{task.fields[remaining_time_label]}"),
        console=console,
        refresh_per_second=1,
    )

    # Process each collection, checking for referential integrity violations
    # (using the reference catalog created earlier to shrink the problem space).
    db = mongo_client.get_database(database_name)
    source_collections_and_their_violations: dict[str, ViolationList] = {}
    with custom_progress as progress:
        for source_collection_name in references.get_source_collection_names():

            # If this source collection is one of the ones the user wanted to skip, skip it now.
            if source_collection_name in names_of_source_collections_to_skip:
                console.print(f"⚠️  [orange][bold]Skipping source collection:[/bold][/orange] {source_collection_name}")
                continue

            collection = db.get_collection(source_collection_name)

            # Prepare the query we will use to fetch the source documents.
            source_field_names = references.get_source_field_names_of_source_collection(source_collection_name)
            or_terms = [{field_name: {'$exists': True}} for field_name in source_field_names]
            query_filter = {'$or': or_terms}
            if verbose:
                console.print(f"{query_filter=}")

            # Ensure the fields we fetch include "id" (so we can produce a more user-friendly report later).
            query_projection = source_field_names + ["id"] if "id" not in source_field_names else source_field_names
            if verbose:
                console.print(f"{query_projection=}")

            # Set up the progress bar for this task.
            num_relevant_documents = collection.count_documents(query_filter)
            task_id = progress.add_task(f"{source_collection_name}",
                                        total=num_relevant_documents,
                                        num_violations=0,
                                        remaining_time_label="remaining")

            # Advance the progress bar by 0 (this makes it so that, even if there are 0 relevant documents,
            # that progress bar does not continue counting its "elapsed time" upward).
            progress.update(task_id, advance=0)

            # Initialize the violation list for this collection.
            source_collections_and_their_violations[source_collection_name] = ViolationList()

            for document in collection.find(query_filter, projection=query_projection):

                # Advance the progress bar for the current task.
                progress.update(task_id,
                                advance=1,
                                num_violations=len(source_collections_and_their_violations[source_collection_name]))

                source_document_object_id = document["_id"]
                source_document_id = document["id"] if "id" in document else None
                for field_name in source_field_names:
                    if field_name in document:
                        target_collection_names = references.get_target_collection_names(source_collection_name,
                                                                                         field_name)

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
                                    source_collections_and_their_violations[source_collection_name].append(violation)
                                    if verbose:
                                        console.print(f"Failed to find document having `id` '{target_id}' "
                                                      f"among collections: {target_collection_names}. "
                                                      f"{violation=}")

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
                                source_collections_and_their_violations[source_collection_name].append(violation)
                                if verbose:
                                    console.print(f"Failed to find document having `id` '{target_id}' "
                                                  f"among collections: {target_collection_names}. "
                                                  f"{violation=}")

            # Update the progress bar to indicate the current task is complete.
            progress.update(task_id, remaining_time_label="done")

    # Close the connection to the MongoDB server.
    mongo_client.close()

    # Print a summary of the violations.
    total_num_violations = 0
    for collection_name, violations in source_collections_and_their_violations.items():
        console.print(f"Number of violations in {collection_name}: {len(violations)}")
        if verbose:
            console.print(violations)
        total_num_violations += len(violations)
    console.print(f"Total violations: {total_num_violations}")

    # Create a violation report in TSV format — for all collections combined.
    # Note: We can still identify a violation's source collection by checking its `source_collection_name` attribute.
    console.print(f"Writing violation report: {violation_report_file_path}")
    all_violations = ViolationList()
    for violations in source_collections_and_their_violations.values():
        all_violations.extend(violations)
    all_violations.dump_to_tsv_file(file_path=violation_report_file_path)


if __name__ == "__main__":
    app()
