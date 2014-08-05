/**
   Copyright (c) 2012-2014, Austin Benson and David Gleich
   All rights reserved.

   This file is part of MRTSQR and is under the BSD 2-Clause License, 
   which can be found in the LICENSE file in the root directory, or at 
   http://opensource.org/licenses/BSD-2-Clause
*/

#ifndef MRTSQR_CXX_TYPEDBYTES_H
#define MRTSQR_CXX_TYPEDBYTES_H

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <string.h>

#if defined(__GNUC__) && __GNUC__ >= 2
# include <byteswap.h>
# define bswap32 bswap_32
# define bswap64 bswap_64
#else
# error no byteswap implemented for your platform yet.
#endif 

#include <string>
#include <vector>

enum TypedBytesType {
  TypedBytesByteSequence = 0,
  TypedBytesByte = 1,
  TypedBytesBoolean = 2,
  TypedBytesInteger = 3,
  TypedBytesLong = 4,
  TypedBytesFloat = 5,
  TypedBytesDouble = 6,
  TypedBytesString = 7,
  TypedBytesVector = 8,
  TypedBytesList = 9,
  TypedBytesMap = 10,
  TypedBytesTypeError = 254,  // a sentinal value for errors
  TypedBytesListEnd = 255,  // a sentinal value for errors
};

typedef int64_t typedbytes_long;
typedef int32_t typedbytes_length;
typedef std::vector<unsigned char> typedbytes_opaque;

// define this type for asserts on the primitive read operations
#define TYPEDBYTES_STRICT_TYPE

class TypedBytesInFile {
 public:    
 TypedBytesInFile(FILE* stream) 
   : stream_(stream), last_code_(TypedBytesTypeError), last_length_(-1)
    {}
    
  // Get the next type code as a supported type.
  TypedBytesType next_type();
    
  // Get the next type code as a raw byte.
  // This command is useful if you are seralizing custom types.
  // This command returns TypedBytesTypeError on an error.
  unsigned char next_type_code();

  // Return the amount of data remaining in a byte-sequence of string.
  typedbytes_length length_remaining() const {
    return last_length_;
  }

  typedbytes_length read_typedbytes_sequence_length();
    
  bool _read_data_block(unsigned char* data, size_t size);

  signed char read_byte();
  bool read_bool();
  float read_float();
  double read_double();
    
  // Read a byte, bool, int, long, or float and convert to double.
  double convert_double();
  // Read a byte, bool, int, or long and convert to long.
  typedbytes_long convert_long();
    
  // Read a byte, bool, int, or long and convert to long.
  int convert_int();

  bool can_be_int(TypedBytesType type) {
    return (type == TypedBytesByte ||
	    type == TypedBytesBoolean ||
	    type == TypedBytesInteger);
  }
  bool can_be_long(TypedBytesType type) {
    return type == TypedBytesLong || can_be_int(type);
  }
  bool can_be_float(TypedBytesType type) {
    return type == TypedBytesFloat || can_be_long(type);
  }
  bool can_be_double(TypedBytesType type) {
    return type == TypedBytesDouble || can_be_float(type);
  }

  bool read_opaque(typedbytes_opaque& buffer);
    
  // Skip the next entry in the TypedBytes file.
  bool skip_next();
        
  int read_int();
  typedbytes_long read_long();

    
  // sequence types

  typedbytes_length read_string_length();

  // Must be called after read_string_length 
  // If size < read_string_length(), then you can call
  // this function multiple times sequentially.
  bool read_string_data(unsigned char* data, size_t size);
    
  bool read_string(std::string& str);
    
  typedbytes_length read_byte_sequence_length();
    
  // Must be called after read_byte_sequence_length
  // If size < read_byte_sequence_length(), then you can call
  // this function multiple times sequentially.
  bool read_byte_sequence(unsigned char* data, size_t size);
  FILE *get_stream() { return stream_; }
  TypedBytesType get_last_code() { return last_code_; }
  typedbytes_length get_last_length() { return last_length_; }

 private:
  FILE* stream_;
  // the last typecode read
  TypedBytesType last_code_;
  // the string/byte-seq length read (decremented by any reading)
  typedbytes_length last_length_;

  bool _read_opaque_primitive(typedbytes_opaque& buffer, 
                              TypedBytesType typecode);
  bool _read_opaque(typedbytes_opaque& buffer, bool list);


  // Read bytes and handle errors.
  // DO NOT call this function directly.
  size_t _read_bytes(void *ptr, size_t nbytes, size_t nelem);

  // Read a 32-bit integer for the length of a string, vector, or map.
  int32_t _read_length();
};

class TypedBytesOutFile {
 public:
 TypedBytesOutFile(FILE *stream)
   : stream_(stream)
  {}
        
  bool write_byte_sequence(unsigned char* bytes, typedbytes_length size) {
    return _write_code(TypedBytesByteSequence) && _write_length(size) &&
      _write_bytes(bytes, sizeof(unsigned char), (size_t) size);
  }
    
  bool write_byte(signed char byte) {
    return _write_code(TypedBytesByte) && _write_bytes(&byte, 1, 1);
  }
  bool write_bool(bool val);
  bool write_int(int val);
  bool write_long(typedbytes_long val);
  bool write_float(float val);
  bool write_double(double val);
  bool write_string(const char* str, typedbytes_length size) {
    return _write_code(TypedBytesString) && _write_length(size) &&
      _write_bytes(str, sizeof(unsigned char), (size_t) size);
  }
  bool write_string_stl(std::string& str) {
    return write_string(str.c_str(), str.size());
  }
    
  bool write_list_start() {
    return _write_code(TypedBytesList);
  }
    
  bool write_list_end() {
    return _write_code(TypedBytesListEnd);
  }
    
  // This function just writes the start of the map code.
  // You are responsible for ensuring subsequent output is correct.
  bool write_map_start(typedbytes_length size) {
    return _write_code(TypedBytesMap) && _write_length(size);
  }
    
  bool write_vector_start(typedbytes_length size) {
    return _write_code(TypedBytesVector) && _write_length(size);
  }
        
  // Write out opaque typedbytes data direct to the stream. 
  // This is just a high level wrapper around fwrite to the stream.
  bool write_opaque_type(unsigned char* bytes, size_t size) {
    return _write_bytes(bytes, 1, size);
  }

 private:
  bool _write_length(typedbytes_length len);
    
  bool _write_bytes(const void* ptr, size_t nbytes, size_t nelem) {
    return fwrite(ptr, nbytes, nelem, stream_) == nelem;
  }
    
  bool _write_code(TypedBytesType t);

  FILE* stream_;
};
        
#endif  // MRTSQR_CXX_TYPEDBYTES_H
