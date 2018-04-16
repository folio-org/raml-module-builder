#!/usr/bin/env bash

help_msg="Some assistance is at https://dev.folio.org/guides/raml-cop"

# The directory to start searching for RAML files.
# Relative to the root of the repository.
ramls_dir="domain-models-api-interfaces/ramls"

# Space-separated list of sub-directory paths that need to be avoided.
prune_dirs="raml-util traits"

if [[ ${BASH_VERSION%%.*} -lt 4 ]]; then
  echo "Requires bash 4+"
  exit 1
fi

if ! cmd=$(command -v raml-cop); then
  echo "raml-cop is not available. Do 'npm install -g raml-cop'"
  echo "${help_msg}"
  exit 1
fi

repo_home="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
cd "${repo_home}" || exit

prune_string=$(printf " -path ${ramls_dir}/%s -o" ${prune_dirs})
mapfile -t raml_files < <(find ${ramls_dir} \( ${prune_string% -o} \) -prune -o -name "*.raml" -print)

if [[ ${#raml_files[@]} -eq 0 ]]; then
  echo "No RAML files found under '${repo_home}/${ramls_dir}'"
  exit 1
fi

result=0

#######################################
# Process a file
#
# Do each file separately to assist with error reporting.
# Even though raml-cop can process multiple files, and be a bit faster,
# when there is an issue then this helps to know which file.
#
#######################################
function process_file () {
  local file="$1"
  ${cmd} "${file}"
  if [[ $? -eq 1 ]]; then
    echo "Errors: ${file}"
    result=1
  fi
}

for f in "${raml_files[@]}"; do
  process_file "$f"
done

if [[ "${result}" -eq 1 ]]; then
  echo "${help_msg}"
fi

exit ${result}
