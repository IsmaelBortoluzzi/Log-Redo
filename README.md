# LOG REDO IMPLEMENTATION

### How To Run The Code:
    
    chmod +x run.sh 
    ./run.sh



### In memory transaction representation:

    {
        'TRANSACTION': {
            'COLUMN': {
                ROW: VALUE,
                ROW: VALUE
            },
            'COLUMN': {
                ROW: VALUE,
                ROW: VALUE
            },
        }
    }

    {
        'T1': {         
            'A': {
                1: 98,
                2: 63
            }
            'B': {
                1: 76,
                2: 57
            }
        },
        'T2': {
            'A': {
                1: 91,
                2: 26
            }
            'B': {
                1: 42,
                2: 98
            }
        }
    }