#!/usr/bin/tcsh

foreach house (`ls`)
    cd $house
    foreach file (`ls *.new`)
        setenv newfile `echo $file | sed -e 's/.new//'`
        mv $file $newfile
    end
    cd ..
end
