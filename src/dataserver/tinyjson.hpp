/*
 * TinyJson 1.3.0
 * A Minimalistic JSON Reader Based On Boost.Spirit, Boost.Any, and Boost.Smart_Ptr.
 *
 * Copyright (c) 2008 Thomas Jansen (thomas@beef.de)
 *
 * Distributed under the Boost Software License, Version 1.0. (See accompanying
 * file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
 *
 * See http://blog.beef.de/projects/tinyjson/ for documentation.
 *
 * (view source with tab-size = 3)
 *
 * 29 Mar 2008 - use strict_real_p for number parsing, small cleanup (Thomas Jansen)
 * 26 Mar 2008 - made json::grammar a template (Boris Schaeling)
 * 20 Mar 2008 - optimized by using boost::shared_ptr (Thomas Jansen)
 * 29 Jan 2008 - Small bugfixes (Thomas Jansen)
 * 04 Jan 2008 - Released to the public (Thomas Jansen)
 * 13 Nov 2007 - initial release (Thomas Jansen) *
 *
 * 29 Mar 2008
 */

#ifndef TINYJSON_HPP
#define TINYJSON_HPP
#include        <boost/version.hpp>
#include        <boost/shared_ptr.hpp>
#include        <boost/any.hpp>
#include        <boost/lexical_cast.hpp>

#if BOOST_VERSION >= 103600
#include        <boost/spirit/include/classic_core.hpp>
#include        <boost/spirit/include/classic_loops.hpp>
#define         BOOST_SPIRIT_NS_ boost::spirit::classic
#else
#include        <boost/spirit/core.hpp>
#include        <boost/spirit/utility/loops.hpp>
#define         BOOST_SPIRIT_NS_ boost::spirit
#endif

#include        <string>
#include        <stack>
#include        <utility>
#include        <deque>
#include        <map>

namespace tair {
   namespace json {
      // ==========================================================================================================
      // ===                                    U N I C O D E _ C O N V E R T                                   ===
      // ==========================================================================================================

      template < typename Char > struct unicodecvt {
         static std::basic_string < Char > convert(int iUnicode) {
            return std::basic_string < Char > (1, static_cast < Char > (iUnicode));
         }
      };


      // ---[ TEMPLATE SPECIALIZATION FOR CHAR ]--------------------------------------------------------------------

      template <> struct unicodecvt <char > {
         static std::string convert(int iUnicode) {
            std::string strString;

            if (iUnicode < 0x0080) {
               // character 0x0000 - 0x007f...

               strString.push_back(0x00 | ((iUnicode & 0x007f) >> 0));
            } else if (iUnicode < 0x0800) {
               // character 0x0080 - 0x07ff...

               strString.push_back(0xc0 | ((iUnicode & 0x07c0) >> 6));
               strString.push_back(0x80 | ((iUnicode & 0x003f) >> 0));
            } else {
               // character 0x0800 - 0xffff...

               strString.push_back(0xe0 | ((iUnicode & 0x00f000) >> 12));
               strString.push_back(0x80 | ((iUnicode & 0x000fc0) >> 6));
               strString.push_back(0x80 | ((iUnicode & 0x00003f) >> 0));
            }

            return strString;
         }
      };


      // ==========================================================================================================
      // ===                                   T H E   J S O N   G R A M M A R                                  ===
      // ==========================================================================================================

      template < typename Char > class grammar:public BOOST_SPIRIT_NS_::grammar < grammar < Char > >
      {
      public:

         // ---[ TYPEDEFINITIONS ]---------------------------------------------------------------------------------

         typedef boost::shared_ptr < boost::any > variant;      //  pointer to a shared variant

         typedef std::stack < variant > stack;  //     a stack of json variants
         typedef std::pair < std::basic_string < Char >, variant > pair;        // a pair as it appears in json

         typedef std::deque < variant > array;  //    an array of json variants
         typedef std::map < std::basic_string < Char >, variant > object;       //    an object with json pairs

      protected:

         // ---[ SEMANTIC ACTION: PUSH A STRING ON THE STACK (AND ENCODE AS UTF-8) ]-------------------------------

         struct push_string {
            stack & m_stack;
            push_string(stack & stack):m_stack(stack) {
            } template < typename Iterator >
            void operator() (Iterator szStart, Iterator szEnd) const {
               // 1: skip the quotes...

               ++szStart;
               --szEnd;

               // 2: traverse through the original string and check for escape codes..

               std::basic_string < char >
                  strString;

               while (szStart < szEnd) {
                  // 2.1: if it's no escape code, just append to the resulting string...

                  if ( *szStart != static_cast < char > ('\\')) {
                     // 2.1.1: append the character...

                     strString.push_back(*szStart);
                  } else {
                     // 2.1.2: otherwise, check the escape code...

                     ++szStart;

                     switch (*szStart) {
                        default:

                           strString.push_back(*szStart);
                           break;

                        case 'b':

                           strString.push_back(static_cast <
                                               char > ('\b'));
                           break;

                        case 'f':

                           strString.push_back(static_cast <
                                               char > ('\f'));
                           break;

                        case 'n':

                           strString.push_back(static_cast <
                                               char > ('\n'));
                           break;

                        case 'r':

                           strString.push_back(static_cast <
                                               char > ('\r'));
                           break;

                        case 't':

                           strString.push_back(static_cast <
                                               char > ('\t'));
                           break;

                        case 'u':
                        {
                           // 2.1.2.1: convert the following hex value into an int...

                           int iUnicode;
                           std::basic_istringstream < Char >
                              (std::basic_string < char >
                               (&szStart[1],
                                4)) >> std::hex >> iUnicode;

                           szStart += 4;

                           // 2.1.2.2: append the unicode int...

                           strString.append(unicodecvt < char >::
                                            convert(iUnicode));
                        }
                     }
                  }

                  // 2.2: go on with the next character...

                  ++szStart;
               }

               // 3: finally, push the string on the stack...

               m_stack.push(variant(new boost::any(strString)));
            }
         };


         // ---[ SEMANTIC ACTION: PUSH A REAL ON THE STACK ]-------------------------------------------------------

         struct push_double {
            stack & m_stack;
            push_double(stack & stack):m_stack(stack) {
            } void operator() (double dValue) const {
               m_stack.push(variant(new boost::any(dValue)));
            }};


         // ---[ SEMANTIC ACTION: PUSH AN INT ON THE STACK ]-------------------------------------------------------

         struct push_int {
            stack & m_stack;
            push_int(stack & stack):m_stack(stack) {
            } void operator() (int64_t iValue) const {
               m_stack.push(variant(new boost::any(iValue)));
            }};


         // ---[ SEMANTIC ACTION: PUSH A BOOLEAN ON THE STACK ]----------------------------------------------------

         struct push_boolean {
            stack & m_stack;
            push_boolean(stack & stack):m_stack(stack) {
            } template < typename Iterator >
            void operator() (Iterator szStart,
                             Iterator /* szEnd */ ) const {
               // 1: push a boolean that is "true" if the string starts with 't' and "false" otherwise...

               m_stack.
                  push(variant
                       (new boost::
                        any(*szStart == static_cast <
                            char > ('t'))));
            }};


         // ---[ SEMANTIC ACTION: PUSH A NULL VALUE ON THE STACK ]-------------------------------------------------

         struct push_null {
            stack & m_stack;
            push_null(stack & stack):m_stack(stack) {
            } template < typename Iterator >
            void operator() (Iterator /* szStart */ ,
                             Iterator /* szEnd */ ) const {
               m_stack.push(variant(new boost::any()));
            }};


         // ---[ SEMANTIC ACTION: CREATE A "JSON PAIR" ON THE STACK ]----------------------------------------------

         struct create_pair {
            stack & m_stack;
            create_pair(stack & stack):m_stack(stack) {
            } template < typename Iterator >
            void operator() (Iterator /* szStart */ ,
                             Iterator /* szEnd */ ) const {
               // 1: get the variant from the stack...

               variant var = m_stack.top();
               m_stack.pop();

               // 2: get the name from the stack...

               std::basic_string < char >
                  strName;

               try {
                  strName =
                     boost::any_cast < std::basic_string <
                     char > >(*m_stack.top());
               } catch(boost::bad_any_cast &) { /* NOTHING */
               }

               m_stack.pop();

               // 3: push a pair of both on the stack...

               m_stack.push(variant(new boost::any(pair(strName, var))));
            }
         };


         // ---[ SEMANTIC ACTION: BEGIN AN ARRAY ]-----------------------------------------------------------------

         class array_delimiter {        /* EMPTY CLASS */
         };

         struct begin_array {
            stack & m_stack;
            begin_array(stack & stack):m_stack(stack) {
            } template < typename Iterator >
            void operator() (Iterator /* cCharacter */ ) const {
               m_stack.push(variant(new boost::any(array_delimiter())));
            }};


         // ---[ SEMANTIC ACTION: CREATE AN ARRAY FROM THE VALUES ON THE STACK ]-----------------------------------

         struct end_array {
            stack & m_stack;
            end_array(stack & stack):m_stack(stack) {
            }
            // - -[ functional operator ] - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            template < typename Iterator >
            void operator() (Iterator /* cCharacter */ ) const {
               // 1: create an array object and push everything in it, that's on the stack...

               variant varArray(new boost::any(array()));

               while (!m_stack.empty()) {
                  // 1.1: get the top most variant of the stack...

                  variant var = m_stack.top();
                  m_stack.pop();

                  // 1.2: is it the end of the array? if yes => break the loop...

                  if (boost::any_cast < array_delimiter > (var.get()) !=
                      NULL) {
                     break;
                  }
                  // 1.3: otherwise, add to the array...
                  boost::any_cast < array >
                     (varArray.get())->push_front(var);
               }

               // 2: finally, push the array at the end of the stack...

               m_stack.push(varArray);
            }
         };


         // ---[ SEMANTIC ACTION: BEGIN AN OBJECT ]----------------------------------------------------------------

         class object_delimiter {       /* EMPTY CLASS */
         };

         struct begin_object {
            stack & m_stack;
            begin_object(stack & stack):m_stack(stack) {
            } template < typename Iterator >
            void operator() (Iterator /* cCharacter */ ) const {
               m_stack.push(variant(new boost::any(object_delimiter())));
            }};


         // ---[ SEMANTIC ACTION: CREATE AN OBJECT FROM THE VALUES ON THE STACK ]----------------------------------

         struct end_object {
            stack & m_stack;
            end_object(stack & stack):m_stack(stack) {
            } template < typename Iterator >
            void operator() (Iterator /* cCharacter */ ) const {
               // 1: create an array object and push everything in it, that's on the stack...

               variant varObject(new boost::any(object()));

               while (!m_stack.empty()) {
                  // 1.1: get the top most variant of the stack...

                  variant var = m_stack.top();
                  m_stack.pop();

                  // 1.2: is it the end of the array? if yes => break the loop...

                  if (boost::any_cast < object_delimiter > (var.get()) !=
                      NULL) {
                     break;
                  }
                  // 1.3: if this is not a pair, we have a problem...
                  pair *pPair = boost::any_cast < pair > (var.get());
                  if (!pPair) {
                     /* BIG PROBLEM!! */

                     continue;
                  }
                  // 1.4: set the child of this object...

                  boost::any_cast < object >
                     (varObject.get())->
                     insert(std::
                            make_pair(pPair->first, pPair->second));
               }

               // 2: finally, push the array at the end of the stack...

               m_stack.push(varObject);
            }
         };

      public:

         stack & m_stack;
         grammar(stack & stack):m_stack(stack) {
         }

         // ---[ THE ACTUAL GRAMMAR DEFINITION ]-------------------------------------------------------------------

         template < typename SCANNER > class definition {
            BOOST_SPIRIT_NS_::rule < SCANNER > m_object;
            BOOST_SPIRIT_NS_::rule < SCANNER > m_array;
            BOOST_SPIRIT_NS_::rule < SCANNER > m_pair;
            BOOST_SPIRIT_NS_::rule < SCANNER > m_value;
            BOOST_SPIRIT_NS_::rule < SCANNER > m_string;
            BOOST_SPIRIT_NS_::rule < SCANNER > m_number;
            BOOST_SPIRIT_NS_::rule < SCANNER > m_boolean;
            BOOST_SPIRIT_NS_::rule < SCANNER > m_null;

         public:

            BOOST_SPIRIT_NS_::rule < SCANNER > const &start() const {
               //return m_object;
               return m_value;
            }
            // - -[ create the definition ] - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            definition(grammar const &self) {
               using namespace BOOST_SPIRIT_NS_;

               // 1: an object is an unordered set of pairs (seperated by commas)...

               m_object
                  = ch_p('{')[begin_object(self.m_stack)] >>
                  !(m_pair >> *(ch_p(',') >> m_pair)) >>
                  ch_p('}')[end_object(self.m_stack)];

               // 2: an array is an ordered collection of values (seperated by commas)...

               m_array
                  = ch_p('[')[begin_array(self.m_stack)] >>
                  !(m_value >> *(ch_p(',') >> m_value)) >>
                  ch_p(']')[end_array(self.m_stack)];

               // 3: a pair is given by a name and a value...

               m_pair = (m_string >> ch_p(':') >> m_value)
                  [create_pair(self.m_stack)];

               // 4: a value can be a string in double quotes, a number, a boolean, an object or an array.

               m_value
                  = m_string
                  | m_number | m_object | m_array | m_boolean | m_null;

               // 5: a string is a collection of zero or more unicode characters, wrapped in double quotes...

               m_string
                  = lexeme_d
                  [(ch_p('"') >> *(((anychar_p -
                                     (ch_p('"') | ch_p('\\')))
                                    | ch_p('\\') >> (ch_p('\"')
                                                     | ch_p('\\')
                                                     | ch_p('/')
                                                     | ch_p('b')
                                                     | ch_p('f')
                                                     | ch_p('n')
                                                     | ch_p('r')
                                                     | ch_p('t')
                                                     | (ch_p('u') >>
                                                        repeat_p(4)
                                                        [xdigit_p])
                                       )
                                      )) >> ch_p('"')
                        )
                   [push_string(self.m_stack)]
                     ];

               // 6: a number is very much like a C or java number...

               m_number = strict_real_p[push_double(self.m_stack)]
                  | int_p[push_int(self.m_stack)];

               // 7: a boolean can be "true" or "false"...

               m_boolean = (str_p("true")
                            | str_p("false")
                  )
                  [push_boolean(self.m_stack)];

               // 8: finally, a value also can be a 'null', i.e. an empty item...

               m_null = str_p("null")
                  [push_null(self.m_stack)];
            }
         };
      };


      // ==========================================================================================================
      // ===                          T H E   F I N A L   P A R S I N G   R O U T I N E                         ===
      // ==========================================================================================================

      template < typename Iterator >
      typename json::grammar < char >::variant
      parse(Iterator const &szFirst, Iterator const &szEnd) {
         // 1: parse the input...

         typename json::grammar < char >::stack st;
         typename json::grammar < char > gr(st);

         BOOST_SPIRIT_NS_::parse_info < Iterator > pi =
            BOOST_SPIRIT_NS_::parse(szFirst, szEnd, gr, BOOST_SPIRIT_NS_::space_p);

         // 2: skip any spaces at the end of the parsed section...

         while ((pi.stop != szEnd)
                && (*pi.stop == static_cast < char > (' '))) {
            ++pi.stop;
         }

         // 3: if the input's end wasn't reached or if there is more than one object on the stack => cancel...

         if ((pi.stop != szEnd) || (st.size() != 1)) {
            return typename json::grammar < char >::variant(new boost::any());
         }
         // 4: otherwise, return the result...

         return st.top();
      }

   };
};

#endif                          // TINYJSON_HPP
