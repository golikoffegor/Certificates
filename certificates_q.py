ch_cert_in_sign = """SELECT
  c.serial_number AS serial_number, cs.human_readable_name_singular AS status
FROM auth.certificates c
    join auth.certificate_statuses cs ON cs.status_id = c.status
WHERE c.serial_number IN ({sert_str})
;"""

