#!/usr/bin/env bash

set -e

echo

default_name=message_store

if [ -z ${DATABASE_USER+x} ]; then
  echo "(DATABASE_USER is not set)"
  user=$default_name
else
  user=$DATABASE_USER
fi
echo "Database user is: $user"

if [ -z ${DATABASE_NAME+x} ]; then
  echo "(DATABASE_NAME is not set)"
  database=$default_name
else
  database=$DATABASE_NAME
fi
echo "Database name is: $database"

if [ -z ${CATEGORY+x} ]; then
  echo "(CATEGORY is not set)"
  category=''
else
  category=$CATEGORY
  echo "Category is: $CATEGORY"
fi

echo
echo "Category Type Summary"
echo "= = ="
echo

if [ -z $category ]; then
  psql $database -U $user -P pager=off -c "SELECT * FROM category_type_summary;"
  psql $database -U $user -P pager=off -c "SELECT COUNT(*) AS total_count FROM messages;"
else
  psql $database -U $user -P pager=off -c "SELECT * FROM category_type_summary WHERE category LIKE '%$category%';"
  psql $database -U $user -P pager=off -c "SELECT COUNT(*) AS total_count FROM messages WHERE category(stream_name) LIKE '%$category%';"
fi
