from pathlib import Path
from typing import List, Optional
from typing_extensions import Annotated

import typer
import linkml_runtime

from refscan.lib.Finder import Finder
from refscan.lib.constants import DATABASE_CLASS_NAME, console
from refscan.lib.helpers import (
    connect_to_database,
    get_collection_names_from_schema,
    derive_schema_class_name_from_document,
    init_progress_bar,
    get_lowercase_key,
)
from refscan.lib.Reference import Reference
from refscan.lib.ReferenceList import ReferenceList
from refscan.lib.Violation import Violation
from refscan.lib.ViolationList import ViolationList

app = typer.Typer(
    help="Scans the NMDC MongoDB database for referential integrity violations.",
    add_completion=False,  # hides the shell completion options from `--help` output
    rich_markup_mode="markdown",  # enables use of Markdown in docstrings and CLI help
)


@app.command("scan")
def scan(
        # Reference: https://typer.tiangolo.com/tutorial/parameter-types/path/
        schema_file_path: Annotated[Path, typer.Option(
            "--schema",
            dir_okay=False,
            writable=False,
            readable=True,
            resolve_path=True,
            help="Filesystem path at which the YAML file representing the schema is located.",
        )],
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
    # Instantiate a `linkml_runtime.SchemaView` bound to the specified schema.
    if verbose:
        console.print(f"Schema YAML file: {schema_file_path}")
    schema_view = linkml_runtime.SchemaView(schema_file_path)

    # Show high-level information about the schema and LinkML runtime.
    console.print(f"Schema version:         {schema_view.schema.version}")
    console.print(f"LinkML runtime version: {linkml_runtime.__version__}")

    # Make a more self-documenting alias for the CLI option that can be specified multiple times.
    names_of_source_collections_to_skip: list[str] = [] if skip_source_collection is None else skip_source_collection

    # Get a list of collection names (technically, `Database` slot names) from the schema.
    # e.g. ["study_set", ...]
    collection_names = get_collection_names_from_schema(schema_view)
    console.print(f"Collections described by schema: {len(collection_names)}")

    # For each collection, determine the names of the classes whose instances can be stored in that collection.
    collection_name_to_class_names = {}  # example: { "study_set": ["Study"] }
    for collection_name in sorted(collection_names):
        slot_definition = schema_view.induced_slot(collection_name, DATABASE_CLASS_NAME)
        name_of_eligible_class = slot_definition.range
        names_of_eligible_classes = schema_view.class_descendants(name_of_eligible_class)  # includes own class name
        collection_name_to_class_names[collection_name] = names_of_eligible_classes

    # Initialize the list of references. A reference is effectively a "foreign key" (i.e. a pointer).
    references = ReferenceList()

    # For each class whose instances can be stored in each collection, determine which of its slots can be a reference.
    sorted_collection_names_to_class_names = sorted(collection_name_to_class_names.items(), key=get_lowercase_key)
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
    if verbose:
        console.print(references.as_table())

    # Get a dictionary that maps source class names to the names of their fields that can contain references.
    reference_field_names_by_source_class_name = references.get_reference_field_names_by_source_class_name()

    # Initialize a progress bar.
    custom_progress = init_progress_bar()

    # Connect to the MongoDB server and verify the database is accessible.
    mongo_client = connect_to_database(mongo_uri, database_name)

    db = mongo_client.get_database(database_name)

    # Make a finder bound to this database.
    # Note: A finder is a wrapper around a database that adds some caching that speeds up searches in some situations.
    finder = Finder(database=db)

    source_collections_and_their_violations: dict[str, ViolationList] = {}
    with custom_progress as progress:

        # Filter out the collections that the schema says can contain references, but that don't exist in the database.
        source_collection_names_in_db = []
        for collection_name in references.get_source_collection_names():
            if db.get_collection(collection_name) is None:
                console.print(f"🤷  [orange]Database lacks collection:[/orange] {collection_name}")
            else:
                source_collection_names_in_db.append(collection_name)

        # Process each collection, checking for referential integrity violations;
        # using the reference catalog created earlier to know which collections can
        # contain "referrers" (documents), which of their slots can contain references (fields),
        # and which collections can contain the referred-to "referees" (documents).
        for source_collection_name in sorted(source_collection_names_in_db):

            # If this source collection is one of the ones the user wanted to skip, skip it now.
            if source_collection_name in names_of_source_collections_to_skip:
                console.print(f"⚠️  [orange][bold]Skipping source collection:[/bold][/orange] {source_collection_name}")
                continue

            collection = db.get_collection(source_collection_name)

            # Prepare the query we will use to fetch documents from this collection. The documents we will fetch are
            # those that have _any_ of the fields (of classes whose instances are allowed to reside in this collection)
            # that the schema allows to contain a reference to an instance.
            source_field_names = references.get_source_field_names_of_source_collection(source_collection_name)
            or_terms = [{field_name: {'$exists': True}} for field_name in source_field_names]
            query_filter = {'$or': or_terms}
            if verbose:
                console.print(f"{query_filter=}")

            # Ensure the fields we fetch include:
            # - "id" (so we can produce a more user-friendly report later)
            # - "type" (so we can map the document to a schema class)
            additional_field_names_for_projection = []
            if "id" not in source_field_names:
                additional_field_names_for_projection.append("id")
            if "type" not in source_field_names:
                additional_field_names_for_projection.append("type")
            query_projection = source_field_names + additional_field_names_for_projection
            if verbose:
                console.print(f"{query_projection=}")

            # Set up the progress bar for the task of scanning those documents.
            num_relevant_documents = collection.count_documents(query_filter)
            task_id = progress.add_task(f"{source_collection_name}",
                                        total=num_relevant_documents,
                                        num_violations=0,
                                        remaining_time_label="remaining")

            # Advance the progress bar by 0 (this makes it so that, even if there are 0 relevant documents, the progress
            # bar does not continue incrementing its "elapsed time" even after a subsequent task has begun).
            progress.update(task_id, advance=0)

            # Initialize the violation list for this collection.
            source_collections_and_their_violations[source_collection_name] = ViolationList()

            # Process each relevant document.
            for document in collection.find(query_filter, projection=query_projection):

                # Get the document's `id` so that we can include it in this script's output.
                source_document_object_id = document["_id"]
                source_document_id = document["id"] if "id" in document else None

                # Get the document's schema class name so that we can interpret its fields accordingly.
                source_class_name = derive_schema_class_name_from_document(schema_view, document)

                # Get the names of that class's fields that can contain references.
                names_of_reference_fields = reference_field_names_by_source_class_name.get(source_class_name, [])

                # Check each field that both (a) exists in the document and (b) can contain a reference.
                for field_name in names_of_reference_fields:
                    if field_name in document:
                        # Determine which collections can contain the referenced document, based upon
                        # the schema class of which this source document is an instance.
                        target_collection_names = references.get_target_collection_names(
                            source_class_name=source_class_name,
                            source_field_name=field_name,
                        )

                        # Handle both the multi-value (array) and the single-value (scalar) case,
                        # normalizing the value or values into a list of values in either case.
                        if type(document[field_name]) is list:
                            target_ids = document[field_name]
                        else:
                            target_id = document[field_name]
                            target_ids = [target_id]  # makes a one-item list

                        for target_id in target_ids:
                            target_exists = finder.check_whether_document_having_id_exists_among_collections(
                                collection_names=target_collection_names,
                                document_id=target_id
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

                # Advance the progress bar to account for the current document's contribution to the violations count.
                progress.update(task_id,
                                advance=1,
                                num_violations=len(source_collections_and_their_violations[source_collection_name]))

            # Update the progress bar to indicate the current task is complete.
            progress.update(task_id, remaining_time_label="done")

    # Close the connection to the MongoDB server.
    mongo_client.close()

    # Create a violation report in TSV format — for all collections combined.
    all_violations = ViolationList()
    for collection_name, violations in sorted(source_collections_and_their_violations.items(), key=get_lowercase_key):
        console.print(f"Number of violations in {collection_name}: {len(violations)}")
        all_violations.extend(violations)
        if verbose:
            console.print(violations)

    console.print(f"Total violations: {len(all_violations)}")
    console.print(f"Writing violation report: {violation_report_file_path}")
    all_violations.dump_to_tsv_file(file_path=violation_report_file_path)


if __name__ == "__main__":
    app()
