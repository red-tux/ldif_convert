# Ldif Conversion

convert_ldif.py is a tool to help convert Ldif files from one Directory server to another using a
text like streaming approach.  Unlike the standard LDIF tools available for python, this does not
attempt to load the full LDIF into memory, thus allowing this to be used on masstive exports.
This is not a purely LDIF approach, or a purely textual approach, but a combination of the two.

By defualt the script loads "settings.yml" as its settings file, however a differnt file can be specified on the command line.

The work done by the script is managed via the settings.yml file.

This is a sample file:

```YAML
input_file: Example.ldif
output_file: test_out.ldif
log_file: test_log.out
case_insensitive: yes
clean_empty: all
IgnoreB64Errors: yes
b64_no_convert:
- jpegPhoto
remove_objects:
  object:
  - atr1
  - atr2
remove_attrs:
  - roomnumber
rename_dn_atrs:
  'ou=admins,dc=example,dc=com':
    Description: DescriptioN
rename_atrs:
  description: DescRiptIon
remove_dn_suffix:
  - ou=badgroup,dc=example,dc=com
dn_remove_attrs:
  uid=tlabonte,ou=People,dc=example,dc=com:
    - facsimiletelephonenumber
schema_regex:
  timestamp:
    find: (\d+)z
    replace: \1Z
schema_validate:
  timestamp: ^\d+Z$
```

| Parameter | Required? | About |
| --------- | ----------| ------|
| input_file|  Yes | The file to import from |
| output_file | Yes | The file to output the modified ldif to |
| log_file | Yes | The file to send logging information to |
| case_insensitive| No | Will all attribute checks be case insensitie, default No |
| clean_empty | No | Set to "all" to purge all empty atributes, anything else is ignored |
| IgnoreB64Errors | No | If the parser cannot convert a base64 entry to unicode, leave as base64 and continue, otherwise |
| b64_no_convert | No | A list of atributes which it will not attempt to decode into unicode, but will leave as base64 |
| remove_objects | No | A list of hashes which will be used to filter out Objects and their associated atributes when found. |
| remove_attrs | No | Atributes which will be removed globally |
| remove_dn_suffix | No | List of suffixes to look for in the DNs which will be skipped/not processed. |
| dn_remove_attrs | No | Atributes which will be removed when the specified DN is found |
| rename_dn_atrs | No | Attributes will be renamed when the specified DN is found |
| rename_atrs | No | Attributes will be renamed globally when found |
| schema_regex | No | A hash of atribute names and find/replace regular expressions to use on the data for the specified atribute.
| schema_validate | No | A list of atributes and an associated regular expression to be used to perform data validation.  If the data does not pass validation it is deleted. |

# Dependencies
This package requires the Python library.
On RHEL systems this can be installed via `yum install PyYAML`
