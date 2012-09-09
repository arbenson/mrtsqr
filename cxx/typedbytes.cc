/**
 * @file typedbytes.cc
 * An implementation of a few of the more complicated typedbytes functions.
 */

#include "typedbytes.h"
#include "stdio.h"

#include <string>
#include <vector>

static inline void push_opaque_typecode(typedbytes_opaque& buffer,
                                        TypedBytesType code) {
  buffer.push_back((unsigned char) code);
}

static inline void push_opaque_bytes(typedbytes_opaque& buffer,
                                     unsigned char* bytes, size_t size) {
  while (size > 0) {
    buffer.push_back(*bytes);
    ++bytes;
    --size;
  }
}
    
static inline void push_opaque_length(typedbytes_opaque& buffer,
                                      typedbytes_length len) {
  len = bswap32(len);
  push_opaque_bytes(buffer, (unsigned char*)&len, sizeof(typedbytes_length));
}
    

bool TypedBytesInFile::_read_opaque_primitive(typedbytes_opaque& buffer, 
                                              TypedBytesType t) {
  // TODO check the fread commands in this function
  unsigned char bytebuf = 0;
  int32_t intbuf = 0;
  int64_t longbuf = 0;
  typedbytes_length len = 0;
    
  // NOTE the typecode has already been pushed
    
  // translate this type to avoid nastiness in the switch.
  if (t >= 50 && t <= 200) {
    t = TypedBytesByteSequence;
  }
  switch (t) {
  case TypedBytesByte:
  case TypedBytesBoolean:
    fread(&bytebuf, sizeof(unsigned char), 1, stream_);
    push_opaque_bytes(buffer, &bytebuf, sizeof(unsigned char));
    break;
            
  case TypedBytesInteger:
  case TypedBytesFloat:
    fread(&intbuf, sizeof(int32_t), 1, stream_);
    push_opaque_bytes(buffer, (unsigned char*) &intbuf, sizeof(int32_t));
    break;
            
  case TypedBytesLong:
  case TypedBytesDouble:
    fread(&longbuf, sizeof(int64_t), 1, stream_);
    push_opaque_bytes(buffer, (unsigned char*) &longbuf, sizeof(int64_t));
    break;
            
  case TypedBytesString:
  case TypedBytesByteSequence:
    len = _read_length();
    while (len > 0) {
      // stream_ to buffer in longbuf bytes at a time.
      if (len >= 8) {
        fread(&longbuf, sizeof(int64_t), 1, stream_);
        push_opaque_bytes(buffer, (unsigned char*) &longbuf, sizeof(int64_t));
        len -= sizeof(int64_t);
      } else {
        fread(&longbuf, len, 1, stream_);
        push_opaque_bytes(buffer, (unsigned char*) &longbuf, len);
        break;
      }
    }
    break;

  default:
    return false;
  }
  return true;
}

bool TypedBytesInFile::_read_opaque(typedbytes_opaque& buffer, bool list) {
  TypedBytesType t = next_type();
  push_opaque_typecode(buffer, t);
  if (t == TypedBytesByteSequence || t == TypedBytesByte ||
      t == TypedBytesBoolean || t == TypedBytesInteger || t==TypedBytesLong ||
      t == TypedBytesFloat || t == TypedBytesDouble || 
      t == TypedBytesString || (t >= 50 && t <= 200)) {
    _read_opaque_primitive(buffer, t);
  } else if (t == TypedBytesVector) {
    typedbytes_length len = read_typedbytes_sequence_length();
    push_opaque_length(buffer, len);
    for (int i = 0; i < len; ++i) {
      _read_opaque(buffer, false);
    }
  } else if(t == TypedBytesMap) {
    typedbytes_length len = read_typedbytes_sequence_length();
    push_opaque_length(buffer, len);
    for (int i = 0; i < len; ++i) {
      _read_opaque(buffer, false);
      _read_opaque(buffer, false);
    }
  } else if (t == TypedBytesList) {
    while (last_code_ != TypedBytesListEnd) {
      _read_opaque(buffer, true);
    }
  } else if (list && t == TypedBytesListEnd) {
    return true;
  } else {
    return false;
  }
  return true;
}

bool TypedBytesInFile::read_opaque(typedbytes_opaque& buffer) {
  return _read_opaque(buffer, false);
}


bool TypedBytesInFile::skip_next() {
  // TODO, rewrite these functions to avoid loading into memory
  typedbytes_opaque value;
  return read_opaque(value);
}

/** Get the next type code as a supported type. */
TypedBytesType TypedBytesInFile::next_type() {
  unsigned char code = next_type_code();
  if (code <= 10 || code == 255) {
    return (TypedBytesType)code;
  } else if (code >= 50 && code <= 200) {
    return TypedBytesByteSequence;
  } else if (code == TypedBytesTypeError) {
    // error flag already set in this case.
    return TypedBytesTypeError;
  } else {
    // TODO set error flag
    return TypedBytesTypeError;
  }
}
    
/** Get the next type code as a raw byte.
 * This command is useful if you are seralizing custom types.
 * This command returns TypedBytesTypeError on an error.
 */
unsigned char TypedBytesInFile::next_type_code() {
  int c = fgetc(stream_);
  // reset last_length_
  last_length_ = -1;
  if (c == EOF) {
    // TODO set error flag
    last_code_ = TypedBytesTypeError;
    return (unsigned char) TypedBytesTypeError;
  } else {
    last_code_ = (TypedBytesType) c;
    return (unsigned char) c;
  }
}
    
/** Read bytes and handle errors.
 * DO NOT call this function directly.
 */
size_t TypedBytesInFile::_read_bytes(void *ptr, size_t nbytes, size_t nelem) {
  size_t nread = fread(ptr, nbytes, nelem, stream_);
  if (nread != nelem) {
    // TODO set error flag and determine more intelligent action.
    assert(0);
  }
  // reset last_length_
  last_length_ = -1;
  return nread;
}
    
/** Read a 32-bit integer for the length of a string, vector, or map. */
int32_t TypedBytesInFile::_read_length() {
  int32_t len = 0;
  _read_bytes(&len, sizeof(int32_t), 1);
  len = bswap32(len);
  return len;
}
    
bool TypedBytesInFile::_read_data_block(unsigned char* data, size_t size) {
  assert(last_length_ >= 0);
  typedbytes_length curlen = last_length_;
  if (size > (size_t) curlen) {
    return false;
  }
  size_t nread = _read_bytes(data, sizeof(unsigned char), (size_t) size);
  // NOTE _read_bytes resets last_length_, so we have to reset it back
  if (nread != size) {
    // TODO update error
    return false;
  }
  last_length_ = curlen - size;
  assert(last_length_ >= 0);
  return true;
}
    
#ifdef TYPEDBYTES_STRICT_TYPE
# define typedbytes_check_type_code(x) (assert((x) == last_code_))
#else
# define typedbytes_check_type_code(x)
#endif    

signed char TypedBytesInFile::read_byte() {
  typedbytes_check_type_code(TypedBytesByte);
  signed char rval = 0;
  _read_bytes(&rval, sizeof(signed char), 1);
  return (rval);
}
    
bool TypedBytesInFile::read_bool() {
  typedbytes_check_type_code(TypedBytesBoolean);
  signed char rval = 0;
  _read_bytes(&rval, sizeof(signed char), 1);
  return (bool)rval;
}
    
float TypedBytesInFile::read_float() {
  typedbytes_check_type_code(TypedBytesFloat);
  int32_t val = 0;
  _read_bytes(&val, sizeof(int32_t), 1);
  val = bswap32(val);
  float rval;
  memcpy(&rval, &val, sizeof(int32_t));
  return rval;
}
    
double TypedBytesInFile::read_double() {
  typedbytes_check_type_code(TypedBytesDouble);
  int64_t val = 0;
  _read_bytes(&val, sizeof(int64_t), 1);
  val = bswap64(val);
  double rval;
  memcpy(&rval, &val, sizeof(int64_t));
  return rval;
}
    
/** Read a byte, bool, int, long, or float and convert to double. */
double TypedBytesInFile::convert_double() {
  if (last_code_ == TypedBytesFloat) {
    return (double) read_float();
  } else if (last_code_ == TypedBytesDouble) {
    return (double) read_double();
  } else {
    return (double) convert_long();
  }
}
    
/** Read a byte, bool, int, or long and convert to long. */
typedbytes_long TypedBytesInFile::convert_long() {
  if (last_code_ == TypedBytesLong) {
    return (long) read_long();
  } else {
    return (long) convert_int();
  }
}
    
/** Read a byte, bool, int, or long and convert to long. */
int TypedBytesInFile::convert_int() {
  if (last_code_ == TypedBytesByte) {
    return (int) read_byte();
  } else if (last_code_ == TypedBytesBoolean) {
    return (int) read_bool();
  } else if (last_code_ == TypedBytesInteger) {
    return (int) read_int();
  } else {
    assert(last_code_ == TypedBytesTypeError);
    return 0;
  }
}
    
bool TypedBytesInFile::can_be_int(TypedBytesType t) {
  switch (t) {
  case TypedBytesByte:
  case TypedBytesBoolean:
  case TypedBytesInteger:
    return true;
  default:
    return false;
  }
}
    
bool TypedBytesInFile::can_be_long(TypedBytesType t) {
  if (t == TypedBytesLong) {
    return true;
  }
  return can_be_int(t);
}
    
bool TypedBytesInFile::can_be_float(TypedBytesType t) {
  if (t == TypedBytesFloat) {
    return true;
  }
  return can_be_long(t);
}
    
bool TypedBytesInFile::can_be_double(TypedBytesType t) {
  if (t == TypedBytesDouble) {
    return true;
  }
  return can_be_float(t);
}
        
int TypedBytesInFile::read_int() {
  typedbytes_check_type_code(TypedBytesInteger);
  int32_t rval = 0;
  _read_bytes(&rval, sizeof(int32_t), 1);
  rval = bswap32(rval);
  return (int) rval;
}
    
typedbytes_long TypedBytesInFile::read_long() {
  typedbytes_check_type_code(TypedBytesLong);
  int64_t rval = 0;
  _read_bytes(&rval, sizeof(int64_t), 1);
  rval = bswap64(rval);
  return (typedbytes_long) rval;
}
    

typedbytes_length TypedBytesInFile::read_string_length() {
  typedbytes_check_type_code(TypedBytesString);
  typedbytes_length len = _read_length();
  last_length_ = len;
  return len;
}
    
/** Must be called after read_string_length 
 * If size < read_string_length(), then you can call
 * this function multiple times sequentially.
 * */
bool TypedBytesInFile::read_string_data(unsigned char* data, size_t size) {
  typedbytes_check_type_code(TypedBytesString);
  return _read_data_block(data, size);
}
    
bool TypedBytesInFile::read_string(std::string& str) {
  typedbytes_check_type_code(TypedBytesString);
  typedbytes_length len = _read_length();
  str.resize(len);
  assert(len >= 0);
  // TODO check for error
  _read_bytes(&str[0], sizeof(unsigned char), (size_t) len);
  return true;
}
    
typedbytes_length TypedBytesInFile::read_byte_sequence_length() {
#ifdef TYPEDBYTES_STRICT_TYPE        
  if (last_code_ == TypedBytesByteSequence || 
      (last_code_ >= 50 && last_code_ <= 200)) {} // do nothing here
  else {
    typedbytes_check_type_code(TypedBytesTypeError); }
#endif
  typedbytes_length len = _read_length();
  last_length_ = len;
  return len;
}
    
/** Must be called after read_byte_sequence_length
 * If size < read_byte_sequence_length(), then you can call
 * this function multiple times sequentially.
 */
bool TypedBytesInFile::read_byte_sequence(unsigned char* data, size_t size) {
#ifdef TYPEDBYTES_STRICT_TYPE        
  if (last_code_ == TypedBytesByteSequence || 
      (last_code_ >= 50 && last_code_ <= 200)) {} // do nothing here
  else { typedbytes_check_type_code(TypedBytesTypeError); }
#endif
  return _read_data_block(data, size);
}

/** The vector and map types are considered sequence types.
 * You are responsible for handling these types yourself.
 */
typedbytes_length TypedBytesInFile::read_typedbytes_sequence_length() {
#ifdef TYPEDBYTES_STRICT_TYPE        
  if (last_code_ == TypedBytesVector || last_code_ == TypedBytesMap) {} 
  else { typedbytes_check_type_code(TypedBytesTypeError); }
#endif        
  return _read_length();
}

bool TypedBytesOutFile::_write_length(typedbytes_length len) {
  len = bswap32(len);
  return fwrite(&len, sizeof(typedbytes_length), 1, stream_) == 1;
}

bool TypedBytesOutFile::_write_code(TypedBytesType t) {
  unsigned char code = (unsigned char) t;
  return _write_bytes(&code, 1, 1);
}

bool TypedBytesOutFile::write_bool(bool val) {
  signed char sval = 0;
  if (val) {
    sval = 1;
  }
  return _write_code(TypedBytesBoolean) && _write_bytes(&sval, 1, 1);
}
    
bool TypedBytesOutFile::write_int(int val) {
  int32_t sval = bswap32(val);
  return _write_code(TypedBytesInteger) &&
    _write_bytes(&sval, sizeof(int32_t), 1);
}
    
bool TypedBytesOutFile::write_long(typedbytes_long val) {
  val = bswap64(val);
  return _write_code(TypedBytesLong) &&
    _write_bytes(&val, sizeof(typedbytes_long), 1);
}
    
bool TypedBytesOutFile::write_float(float val) {
  int32_t sval = 0;
  memcpy(&sval, &val, sizeof(int32_t));
  sval = bswap32(sval);
  return _write_code(TypedBytesFloat) && _write_bytes(&sval, sizeof(int32_t), 1);
}
    
bool TypedBytesOutFile::write_double(double val) {
  int64_t sval = 0;
  memcpy(&sval, &val, sizeof(int64_t));
  sval = bswap64(sval);
  return _write_code(TypedBytesDouble) &&
    _write_bytes(&sval, sizeof(int64_t), 1);
}

