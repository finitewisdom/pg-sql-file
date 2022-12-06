    select code, name
      from demo.state
     where code = any( $codes );
