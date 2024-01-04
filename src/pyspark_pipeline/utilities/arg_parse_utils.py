from argparse import ArgumentParser, Namespace
from distutils.util import strtobool


def get_base_arg_parser() -> ArgumentParser:
    parser = ArgumentParser(description="Generate specs for Hive tables")

    parser.add_argument(
        "-s",
        "--settings-yaml",
        type=str,
        action="store",
        required=True,
        help="required: yaml string or path to yaml file specifying job settings",
    )
    parser.add_argument(
        "-O",
        "--override-settings",
        metavar="O",
        type=str,
        action="store",
        default="",
        required=False,
        help="path to yaml for generic settings for the job",
    )
    parser.add_argument(
        "-e",
        "--rebuild-conda-env",
        type=lambda x: bool(strtobool(x)),
        action="store",
        default=True,
        required=False,
        help="specifies whether to use cached conda env or create new archive",
    )
    parser.add_argument(
        "-z",
        "--zipped-environment-path",
        metavar="z",
        type=str,
        action="store",
        default=None,
        required=False,
        help="A path to a preexisting conda environment",
    )
    parser.add_argument(
        "-t",
        "--table-tag",
        metavar="t",
        type=str,
        action="store",
        default="",
        required=False,
        help="overrides table tag in settings",
    )
    parser.add_argument(
        "-l",
        "--spark-log-level",
        metavar="l",
        type=str,
        action="store",
        default="WARN",
        required=False,
        help="The logging level of spark",
    )
    parser.add_argument(
        "-M",
        "--spark-submit-mode",
        metavar="M",
        type=str,
        action="store",
        choices=["cluster", "client"],
        required=False,
        help="spark-submit method (either 'client' or 'cluster' accepted)",
    )
    parser.add_argument(
        "-id",
        "--run-id",
        metavar="id",
        type=str,
        action="store",
        default="",
        required=False,
        help="run id that will be stored in each output table and audit table",
    )
    parser.add_argument(
        "-L",
        "--log-filtered-values",
        metavar="L",
        type=lambda x: bool(strtobool(x)),
        action="store",
        default=False,
        required=False,
        help="Option to log filtered values where check is implemented",
    )

    return parser


def get_incremental_base_arg_parser() -> ArgumentParser:
    parser = get_base_arg_parser()
    parser.add_argument(
        "-I",
        "--incremental_processing_type",
        metavar="I",
        type=str,
        action="store",
        default=None,
        required=False,
        nargs="?",
        choices=("update", "changes_only"),
        help="specifies the type of incremental processing to be performed ",
    )
    parser.add_argument(
        "-S",
        "--incremental-load-start-date",
        metavar="S",
        type=str,
        action="store",
        default="",
        required=False,
        help="the min storage date (yyyy-mm-dd) for incremental load",
    )
    parser.add_argument(
        "-E",
        "--incremental-load-end-date",
        metavar="E",
        type=str,
        action="store",
        default="",
        required=False,
        help="the max storage date (yyyy-mm-dd) for incremental load",
    )
    return parser


def parse_base_args() -> Namespace:
    parser = get_base_arg_parser()
    return parser.parse_args()


def parse_job_args() -> Namespace:
    parser = get_incremental_base_arg_parser()
    parser.add_argument(
        "--skip-tags",
        type=lambda x: bool(strtobool(x)),
        action="store",
        default=False,
        required=False,
        help="Skip `tag` column generation",
    )

    return parser.parse_args()


def parse_subset_tables_by_csv_of_ids_args() -> Namespace:
    parser = get_base_arg_parser()
    parser.add_argument(
        "-N",
        "--subset-name",
        metavar="N",
        type=str,
        action="store",
        required=True,
        help="specifies the name of the subset created",
    )
    parser.add_argument(
        "-c",
        "--id-csv-path",
        metavar="p",
        type=str,
        action="store",
        required=True,
        help="specifies path for csv containing column of ids",
    )

    return parser.parse_args()


def parse_subset_tables_by_hive_of_ids_args() -> Namespace:
    parser = get_base_arg_parser()
    parser.add_argument(
        "-N",
        "--subset-name",
        metavar="N",
        type=str,
        action="store",
        required=True,
        help="specifies the name of the subset created",
    )

    return parser.parse_args()


def parse_subset_tables_by_number_of_ids_args() -> Namespace:
    parser = get_base_arg_parser()
    parser.add_argument(
        "-n",
        "--number-of-ids",
        metavar="r",
        type=int,
        action="store",
        default=10,
        required=False,
        help="specifies the maximum number of ids in tables",
    )
    parser.add_argument(
        "-x",
        "--exclusion_id_csv_path",
        metavar="p",
        type=str,
        action="store",
        required=False,
        help=(
            "specifies path for csv containing column of "
            "ids that will be excluded from the data"
        ),
    )
    parser.add_argument(
        "-i",
        "--inclusion_id_csv_path",
        metavar="p",
        type=str,
        action="store",
        required=False,
        help=(
            "specifies path for csv containing column of "
            "ids that will be added to the subset"
        ),
    )

    return parser.parse_args()


def parse_deidentify_table_args() -> Namespace:
    parser = get_base_arg_parser()
    parser.add_argument(
        "-m",
        "--use-existing-member-info-table",
        metavar="m",
        type=bool,
        action="store",
        default=False,
        required=False,
        help="if true uses an existing member info table",
    )

    return parser.parse_args()


def parse_subset_by_table_column_string_args() -> Namespace:
    parser = get_base_arg_parser()
    parser.add_argument(
        "-d",
        "--dataframe",
        metavar="d",
        type=str,
        action="store",
        required=True,
        help="dataframe that will be used for query",
    )
    parser.add_argument(
        "-c",
        "--column",
        metavar="c",
        type=str,
        action="store",
        required=True,
        help="column that will be used for query",
    )
    parser.add_argument(
        "-p",
        "--pattern",
        metavar="c",
        type=str,
        action="store",
        required=True,
        help="pattern that will be used for query",
    )

    parser.add_argument(
        "-N",
        "--subset-name",
        metavar="N",
        type=str,
        action="store",
        required=True,
        help="specifies the name of the subset created",
    )

    return parser.parse_args()


def parse_subset_population_args() -> Namespace:
    parser = get_base_arg_parser()
    parser.add_argument(
        "-n",
        "--number-of-ids",
        metavar="r",
        type=int,
        action="store",
        default=10,
        required=False,
        help="specifies the maximum number of ids in tables",
    )
    parser.add_argument(
        "-c",
        "--max-extra-keys",
        metavar="c",
        type=int,
        action="store",
        default=1,
        required=False,
        help=(
            "specifies the maximum number of clm_adjstmnt_key for each "
            "type of claim_derived_resource and the number of "
            "lab_rqst_key per id"
        ),
    )
    parser.add_argument(
        "-k",
        "--include-extra-keys",
        metavar="k",
        type=lambda x: bool(strtobool(x)),
        action="store",
        default=True,
        required=False,
        help=(
            "specifies whether extra keys like clm_adjstmnt_key "
            "are included in the subset population. If False only  "
            "member identifiers will be included"
        ),
    )

    return parser.parse_args()
