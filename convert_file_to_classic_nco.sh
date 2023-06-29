foreach i (`ls emiss*.nc`)
  nccopy -k 2 $i 64/$i
end
#foreach i (`ls emiss*.nc`)
#  nccopy -k 1 $i classic/$i
#end
#foreach i (`ls emiss*.nc`)
#  nccopy -k 4 $i classic_4/$i
#end
