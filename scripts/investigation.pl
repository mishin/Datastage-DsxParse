#################################################################
#### STAGE: CLIENT_BCE
## Operator
copy
## General options
[ident('CLIENT_BCE')]
## Inputs
0< [] 'T106:L119.v'
## Outputs
0>| [ds] '[&\"psProjectsPath.ProjectFilePath\"]FICLI_IS_CLIENT_BCE.ds'
;

'L129.ADDRESS_COUNTRY_CODE',
SourceColumn = L129.ADDRESS_COUNTRY_CODE

|- Name = L129
|- OLEType = CTrxOutput

ColumnReference = ADDRESS_COUNTRY_CODE
   |     |  |- Derivation = Trim(Left(L106.ADDRESS_COUNTRY_CODE, 5))
       |     |  |- ParsedDerivation = Trim(Left(L106.ADDRESS_COUNTRY_CODE, 5))
       SourceColumn = L106.ADDRESS_COUNTRY_CODE
       Name = L106
       
          |  |  |- Name = L106
   |  |  |- OLEType = CTrxOutput
   
   ColumnReference = ADDRESS_COUNTRY_CODE
   |     |  |- Derivation = L105.ADDRESS_COUNTRY_CODE
   ParsedDerivation = L105.ADDRESS_COUNTRY_CODE
   SourceColumn = L105.ADDRESS_COUNTRY_CODE
   
    |- ParsedDerivation = L104.ADDRESS_COUNTRY_CODE
   |     |  |- SourceColumn = L104.ADDRESS_COUNTRY_CODE
          |  |  |- Name = L104
          
            |  |- Name = L104
   |  |  |- OLEType = CCustomOutput
   
    |- ParsedDerivation = L103.ADDRESS_COUNTRY_CODE
   |     |  |- SourceColumn = L103.ADDRESS_COUNTRY_CODE
   
   
            |  |- Name = L103
            - SourceColumn = L102.ADDRESS_COUNTRY_CODE
            Name = L102
            Name = L101
            Name = L100
            
            
               |- 21

Это самое, что ни на есть поле!!
       |     |  |- AllowCRLF = 0
       |     |  |- ArrayHandling = 0
       |     |  |- ColumnReference = ADDRESS_COUNTRY_CODE
       |     |  |- Description = <none>
       |     |  |- DisplaySize = 50
       |     |  |- ExtendedPrecision = 0
       |     |  |- Group = 0
       |     |  |- KeyPosition = 0
       |     |  |- LevelNo = 0
       |     |  |- Name = ADDRESS_COUNTRY_CODE
       |     |  |- Nullable = 1
       |     |  |- Occurs = 0
       |     |  |- OccursVarying = 0
       |     |  |- PadChar
       |     |  |- PadNulls = 0
       |     |  |- PKeyIsCaseless = 0
       |     |  |- Precision = 50
       |     |  |- Scale = 0
       |     |  |- SCDPurpose = 0
       |     |  |- SignOption = 0
       |     |  |- SortingOrder = 0
       |     |  |- SortKey = 0
       |     |  |- SortType = 0
       |     |  |- SqlType = 12
       |     |  |- SyncIndicator = 0
       |     |  |- TableDef = PlugIn\\ORAOCI9\\IPS.FICLI_CLIENTS
       |     |  `- TaggedSubrec = 0
            
            101
            
            
            
       Name = L105
       
       Name = L105
   |  |  |- OLEType = CCustomOutput
   
     Columns = COutputColumn
       |  |  |- ErrorPin = 0
       |  |  |- Identifier = V0S39P14
       |  |  |- LeftTextPos = 2256
       |  |  |- Name = L129
       |  |  |- OLEType = CTrxOutput
       |  |  |- Partner = V0S139|V0S139P1
       |  |  |- Readonly = 0
       |  |  |- Reject = 0
       |  |  |- RowLimit = 0
       |  |  `- TopTextPos = 648

   |  |- identifier = V0S39P14
   
     |  |- ColumnReference = ADDRESS_COUNTRY_CODE
   |     |  |- Derivation = Trim(Left(L106.ADDRESS_COUNTRY_CODE, 5))
   
   - ParsedDerivation = Trim(Left(L106.ADDRESS_COUNTRY_CODE, 5))   
   
   Derivation = L109.ADDRESS_COUNTRY_CODE
   
   ParsedDerivation = L109.ADDRESS_COUNTRY_CODE
   
   
   