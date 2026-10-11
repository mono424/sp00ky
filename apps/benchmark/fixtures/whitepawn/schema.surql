-- Synthetic WhitePawn-inspired library. All rows are disposable benchmark data.
-- FULL permissions isolate scheduler/SSP cost from application authorization.
DEFINE TABLE game_database SCHEMAFULL PERMISSIONS FULL;
DEFINE FIELD owner ON TABLE game_database TYPE string;
DEFINE FIELD name ON TABLE game_database TYPE string;
DEFINE INDEX game_database_owner ON TABLE game_database FIELDS owner;

DEFINE TABLE player_name SCHEMAFULL PERMISSIONS FULL;
DEFINE FIELD owner ON TABLE player_name TYPE string;
DEFINE FIELD name ON TABLE player_name TYPE string;

DEFINE TABLE game SCHEMAFULL PERMISSIONS FULL;
DEFINE FIELD owner ON TABLE game TYPE string;
DEFINE FIELD database ON TABLE game TYPE record<game_database>;
DEFINE FIELD white ON TABLE game TYPE record<player_name>;
DEFINE FIELD black ON TABLE game TYPE record<player_name>;
DEFINE FIELD date ON TABLE game TYPE datetime;
DEFINE FIELD result ON TABLE game TYPE string;
DEFINE FIELD eco ON TABLE game TYPE string;
DEFINE FIELD source ON TABLE game TYPE string;
DEFINE FIELD avatar ON TABLE game TYPE string;
DEFINE FIELD sort_index ON TABLE game TYPE int;
DEFINE FIELD status ON TABLE game TYPE string;
DEFINE FIELD speed ON TABLE game TYPE string;
DEFINE FIELD pgn ON TABLE game TYPE option<string>;
DEFINE INDEX game_sort_index ON TABLE game FIELDS sort_index;
DEFINE INDEX game_database_sort ON TABLE game FIELDS database, sort_index;
DEFINE INDEX game_owner_sort ON TABLE game FIELDS owner, sort_index;
