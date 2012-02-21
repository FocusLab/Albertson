Albertson
=========

A library for super easy to use, reliable, and scalable counters.

Homepage: [https://github.com/FocusLab/Albertson][3]

**Albertson is in an alpha state and probably shouldn't be used in
production by anybody just yet.**

![](http://upload.wikimedia.org/wikipedia/en/7/79/The_Simpsons-Jeff_Albertson.png)

*"Worst library ever!"*




Why
---
Creating counters that handle incrementing at high levels of concurrency while
also being highly available and fault tolerant is *really* hard.  Thankfully,
Amazon has solved these really hard bits with their [DynamoDB][2] service.

Albertson provides a simple, clean, and easy to use, Python interface to
DynamoDb for this specific use case.


Who
---
Albertson was created by the dev team at [FocusLab][1].


What's with the name?
---------------------
Internally at FocusLab Albertson is used for real-time, authoritative, counts
that are often used to correct less frequently updated counts throughout our
system.  Accordingly, we've named the library after our favorite fictional
pedant, Comic Book Guy a.k.a. Jeff Albertson.


[1]:    https://www.focuslab.io
[2]:    http://aws.amazon.com/dynamodb/
[3]:    https://github.com/FocusLab/Albertson
