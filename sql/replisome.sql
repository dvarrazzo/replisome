create function replisome_version() returns text
as 'MODULE_PATHNAME'
language c immutable strict;
