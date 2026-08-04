select * from {{ source('admin', 'dba_sys_privs') }}
where PRIVILEGE like 'SELECT ANY TABLE'