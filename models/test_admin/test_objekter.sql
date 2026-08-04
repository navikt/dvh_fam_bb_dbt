select * from {{ source('admin_dmo', 'dmo_dstr_objects_to_team') }}
where schema_name like '%FAM%'