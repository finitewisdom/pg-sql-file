    select code, name
      from demo.state
     where upper( name ) like ( upper( $term ) || '%' );
