SELECT a.attname
  FROM (
         SELECT indrelid, indkey, generate_subscripts(indkey, $1) idx
           FROM pg_index
          WHERE indrelid = $2::regclass
            AND indisprimary
       ) i
  JOIN pg_attribute a
    ON a.attrelid = i.indrelid
   AND a.attnum = i.indkey[i.idx]
 ORDER BY i.idx /*action='show',namespaced_controller='api%2Fv1%2Ftimelines%2Fhome'*/;
